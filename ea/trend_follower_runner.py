import argparse
import os
from dataclasses import dataclass
from datetime import datetime

import pendulum
from xtb.wrapper.xtb_client import APIClient, loginCommand

from ea.messaging.slack import SlackService
from ea.misc.logger import logger
from ea.strategies.indicators.trend_follower import TrendFollower
from ea.trading.expert_advisor import ExpertAdvisor, ExpertAdvisorSettings
from ea.trading.order import OrderMode, OrderType, OrderWrapper


@dataclass
class EARunnerSettings:
    client: APIClient
    slack: SlackService
    symbol: str
    period: int
    # used for scipy.signal argrelextrema calculation
    extreme_candles_range: int
    # used for Beta calculation
    beta_extreme_count: int
    # n-element count for lower/upper bounds
    rolling_window: int
    stop_loss_factor: float
    take_profit_factor: float
    run_at: datetime


class EARunner:
    def __init__(self, settings: EARunnerSettings):
        self._settings = settings

    def get_scenario_name(self):
        return f'symbol:{self._settings.symbol}-' \
               f'period:{self._settings.period}-' \
               f'extreme_candles_range:{self._settings.extreme_candles_range}-' \
               f'beta_extreme_count:{self._settings.beta_extreme_count}-' \
               f'rolling_window:{self._settings.rolling_window}-' \
               f'stop_loss_factor:{self._settings.stop_loss_factor}-' \
               f'take_profit_factor:{self._settings.take_profit_factor}-'

    def get_price(self, price_value, precision):
        price = round(price_value, precision)

        return price

    def get_stop_loss(self, position_side, value_for_bullish, value_for_bearish, precision):
        calculated_stop_loss = value_for_bullish \
            if position_side == 'bullish' \
            else value_for_bearish
        stop_loss = round(calculated_stop_loss, precision)

        return stop_loss

    def get_take_profit(self, position_side, price, lower_bound, upper_bound, precision):
        take_profit_range = \
            (upper_bound - lower_bound)

        take_profi_range_with_factor = \
            take_profit_range * self._settings.take_profit_factor

        take_profit_at = price + take_profi_range_with_factor \
            if position_side == 'bullish' \
            else price - take_profi_range_with_factor

        take_profit = round(take_profit_at, precision)

        return take_profit

    def time_mod(self, time, delta, epoch=None):
        if epoch is None:
            epoch = datetime(1970, 1, 1, tzinfo=time.tzinfo)
        return (time - epoch) % delta

    def time_floor(self, time, delta, epoch=None):
        mod = self.time_mod(time, delta, epoch)
        return time - mod

    def get_expiration(self, current_time):
        # whole day plus 5 minutes
        delta = (60 * 60 * 24 + 5) * 1000
        # add 5 minutes apart from next candle itself
        # delta = (60 * (self._settings.period + 5)) * 1000

        return current_time + delta

    def prepare_order(self, symbol_info: dict, order_input: dict) -> OrderWrapper:
        precision = symbol_info['precision']
        spreadRaw = symbol_info['spreadRaw']

        order_type = OrderType.OPEN.value
        order_mode = OrderMode.BUY_STOP.value \
            if order_input['beta_sign'] == 1 \
            else OrderMode.SELL_STOP.value

        price = self.get_price(order_input['indicator'] + spreadRaw, precision) \
            if order_input['position_side'] == 'bullish' \
            else self.get_price(order_input['indicator'], precision)
        expiration = self.get_expiration(symbol_info['time'])
        stop_loss = self.get_stop_loss(
            order_input['position_side'],
            order_input['rolling_n_low'],
            order_input['rolling_n_high'],
            precision
        )
        take_profit = self.get_take_profit(
            order_input['position_side'],
            price,
            order_input['rolling_n_low'],
            order_input['rolling_n_high'],
            precision
        )

        return OrderWrapper(
            order_type=order_type,
            order_mode=order_mode,
            price=price,
            symbol=self._settings.symbol,
            expiration=expiration,
            stop_loss=stop_loss,
            take_profit=take_profit,
            volume=symbol_info['lotMin'],
            custom_comment=order_input['custom_comment']
        )

    def start(self):
        scenario_name = self.get_scenario_name()
        logger.info(f'Running EA for scenario: {scenario_name}')

        # Engulfing
        strategy_settings = dict(
            symbol=self._settings.symbol,
            period=self._settings.period,
            extreme_candles_range=self._settings.extreme_candles_range,
            beta_extreme_count=self._settings.beta_extreme_count,
            rolling_window=self._settings.rolling_window,
            stop_loss_factor=self._settings.stop_loss_factor,
            take_profit_factor=self._settings.take_profit_factor
        )

        strategy = TrendFollower(strategy_settings)

        ea_settings = ExpertAdvisorSettings(
            client=self._settings.client,
            symbol=self._settings.symbol,
            period=self._settings.period,
            scenario_name=scenario_name,
            run_at=self._settings.run_at
        )
        ea = ExpertAdvisor(ea_settings)

        raw_df = ea.from_api()
        analyzed_df = strategy.analyze(raw_df)

        # How far from last extreme
        current_extreme_index = analyzed_df.groupby('current_extreme_on')['current_extreme_on'].idxmin()[-1]
        since_extreme_index = analyzed_df.index[-1] - current_extreme_index

        last_row = analyzed_df.iloc[-1]
        order_input = last_row.to_dict()
        # TODO - scenario_name + something makes an order unique - effectively letting multiple orders
        order_input['custom_comment'] = scenario_name
        symbol_info = ea.get_symbol()
        precision = symbol_info['precision']
        spreadRaw = symbol_info['spreadRaw']

        current_trades = ea.get_open_trades(scenario_name, opened_only=False)
        open_orders = [order for order in current_trades if not order['expiration']]
        pending_orders = [order for order in current_trades if order['expiration']]
        # Modifying Order
        # PENDING
        if len(pending_orders) != 0:
            for order in pending_orders:
                logger.info(f"Modifying PENDING order {order}")
                current_trade_market = ea.get_current_trade_market(order['cmd'])
                logger.info(f"Order side {current_trade_market}")

                price = self.get_price(order_input['indicator'] + spreadRaw, precision) \
                    if current_trade_market == 'bullish' \
                    else self.get_price(order_input['indicator'], precision)
                stop_loss = self.get_stop_loss(
                    current_trade_market,
                    order_input['rolling_n_low'],
                    order_input['rolling_n_high'],
                    precision
                )
                take_profit = self.get_take_profit(
                    current_trade_market,
                    price,
                    order_input['rolling_n_low'],
                    order_input['rolling_n_high'],
                    precision
                )
                modification_order = OrderWrapper(
                    order_mode=order['cmd'],
                    price=price,
                    symbol=self._settings.symbol,
                    order_type=OrderType.MODIFY.value,
                    expiration=order['expiration'],
                    order_number=order['order'],
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    volume=order['volume'],
                    custom_comment=order['customComment']
                )
                modification_resp = ea.modifyPosition(modification_order)
                logger.info(modification_resp)
                self._settings.slack.send(
                    f'Modifying PENDING order {order};'
                    f'{scenario_name}: '
                    f'{str(modification_resp)}'
                )
        # OPEN
        elif (len(open_orders) != 0):
            for order in open_orders:
                logger.info(f"Modifying OPEN order {order}")
                current_trade_market = ea.get_current_trade_market(order['cmd'])
                logger.info(f"Order side {current_trade_market}")

                price = self.get_price(order_input['indicator'] + spreadRaw, precision) \
                    if current_trade_market == 'bullish' \
                    else self.get_price(order_input['indicator'], precision)
                stop_loss = self.get_stop_loss(
                    current_trade_market,
                    order_input['rolling_n_low'],
                    order_input['rolling_n_high'],
                    precision
                )
                take_profit = self.get_take_profit(
                    current_trade_market,
                    price,
                    order_input['rolling_n_low'],
                    order_input['rolling_n_high'],
                    precision
                )
                modification_order = OrderWrapper(
                    order_mode=order['cmd'],
                    price=order['open_price'],
                    symbol=self._settings.symbol,
                    order_type=OrderType.MODIFY.value,
                    expiration=order['expiration'],
                    order_number=order['order'],
                    stop_loss=stop_loss,
                    take_profit=order['tp'],
                    volume=order['volume'],
                    custom_comment=order['customComment']
                )
                modification_resp = ea.modifyPosition(modification_order)
                logger.info(modification_resp)
                self._settings.slack.send(
                    f'Modifying OPEN order {order};'
                    f'{scenario_name}: '
                    f'{str(modification_resp)}'
                )
        # Place new orders
        elif (len(open_orders) == 0) \
                & (last_row['beta_sign'] == 1) \
                & (last_row['indicator'] >= last_row['indicator_lead']) \
                & (since_extreme_index == 2):
            # Bullish
            logger.info('Placing BUY order')
            order_input['position_side'] = 'bullish'
            prepared_order = self.prepare_order(symbol_info, order_input)
            try:
                order_resp = ea.open_order_on_signal(prepared_order, ea.execute_tradeTransaction)
            except Exception as e:
                raise
            finally:
                logger.info(order_resp)
                self._settings.slack.send(
                    f'Placing BUY order for:{scenario_name}:'
                    f'{str(order_resp)}'
                )
        elif (len(open_orders) == 0) \
                & (last_row['beta_sign'] == -1) \
                & (last_row['indicator'] <= last_row['indicator_lead']) \
                & (since_extreme_index == 2):
            # Bearish
            logger.info('Placing SELL order')
            order_input['position_side'] = 'bearish'
            prepared_order = self.prepare_order(ea.get_symbol(), order_input)
            try:
                order_resp = ea.open_order_on_signal(prepared_order, ea.execute_tradeTransaction)
            except Exception as e:
                raise
            finally:
                logger.info(order_resp)
                self._settings.slack.send(
                    f'Placing SELL order for:{scenario_name}:'
                    f'{str(order_resp)}'
                )
        else:
            logger.info('No signal')


if __name__ == "__main__":
    local_tz = pendulum.timezone('Europe/Warsaw')
    run_at = pendulum.now(tz=local_tz)
    running_at_string = run_at.strftime("%d/%m/%Y %H:%M:%S")
    logger.info(f'Process started at: {running_at_string}')

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--symbol', type=str, required=True)
    parser.add_argument('-pd', '--period', type=int, required=True)
    parser.add_argument('-ecr', '--extreme_candles_range', type=int, required=True)
    parser.add_argument('-bec', '--beta_extreme_count', type=int, required=True)
    parser.add_argument('-rw', '--rolling_window', type=int, required=True)
    parser.add_argument('-slf', '--stop_loss_factor', type=float, required=True)
    parser.add_argument('-tpf', '--take_profit_factor', type=float, required=True)

    args = parser.parse_args()
    user_id = os.getenv('XTB_API_USER')
    password = os.getenv('XTB_API_PASSWORD')
    symbol = args.symbol
    period = args.period
    extreme_candles_range = args.extreme_candles_range
    beta_extreme_count = args.beta_extreme_count
    rolling_window = args.rolling_window
    stop_loss_factor = args.stop_loss_factor
    take_profit_factor = args.take_profit_factor

    client = APIClient()
    loginResponse = client.execute(loginCommand(userId=user_id, password=password))

    if not loginResponse['status']:
        logger.error('Login failed. Error code: {0}'.format(loginResponse['errorCode']))
    else:
        slack = SlackService(os.getenv('SLACK_URL'))
        ea_runner_settings = EARunnerSettings(
            client,
            slack,
            symbol,
            period,
            extreme_candles_range,
            beta_extreme_count,
            rolling_window,
            stop_loss_factor,
            take_profit_factor,
            run_at
        )
        EARunner(ea_runner_settings).start()

    finishing_at_string = pendulum.now(tz=local_tz).strftime("%d/%m/%Y %H:%M:%S")
    logger.info(f'Process ended at: {finishing_at_string}')

    client.commandExecute('logout')
    client.disconnect()
