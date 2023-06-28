import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta

import pendulum
from xtb.wrapper.xtb_client import APIClient, loginCommand

from ea.messaging.slack import SlackService
from ea.misc.logger import logger
from ea.strategies.indicators.candles.long_shadow import LongShadow
from ea.trading.expert_advisor import ExpertAdvisor, ExpertAdvisorSettings
from ea.trading.order import OrderMode, OrderType, OrderWrapper


@dataclass
class EARunnerSettings:
    client: APIClient
    slack: SlackService
    symbol: str
    period: int
    candle_body_ratio: float
    candle_height_quantile: float
    regression_candles_count: int
    stop_loss_factor: float
    take_profit_factor: float
    run_at: datetime


class EARunner:
    def __init__(self, settings: EARunnerSettings):
        self._settings = settings

    def get_scenario_name(self):
        return f'symbol:{self._settings.symbol}-' \
               f'period:{self._settings.period}-' \
               f'candle_body_ratio:{self._settings.candle_body_ratio}-' \
               f'candle_height_quantile:{self._settings.candle_height_quantile}-' \
               f'regression_candles_count:{self._settings.regression_candles_count}'

    def time_mod(self, time, delta, epoch=None):
        if epoch is None:
            epoch = datetime(1970, 1, 1, tzinfo=time.tzinfo)
        return (time - epoch) % delta

    def time_floor(self, time, delta, epoch=None):
        mod = self.time_mod(time, delta, epoch)
        return time - mod

    def get_expiration(self, current_time):
        delta = self._settings.period * 4
        return current_time + timedelta(minutes=delta)

    def prepare_order(self, symbol_info: dict, order_input: dict) -> OrderWrapper:
        precision = symbol_info['precision']

        order_type = OrderType.OPEN.value
        order_mode = OrderMode.BUY_STOP.value \
            if order_input['position_side'] == 'bullish' \
            else OrderMode.SELL_STOP.value
        price = round(order_input['high'] + symbol_info['spreadRaw'], precision) \
            if order_input['position_side'] == 'bullish' \
            else round(order_input['low'] - symbol_info['spreadRaw'], precision)

        price_range = (order_input['high'] - order_input['low'])

        stop_loss = round(price - price_range * self._settings.stop_loss_factor, precision) \
            if order_input['position_side'] == 'bearish' \
            else round(price + price_range * self._settings.stop_loss_factor, precision)

        take_profit_range = price_range * self._settings.take_profit_factor
        take_profit_at = price + take_profit_range \
            if order_input['position_side'] == 'bullish' \
            else price - take_profit_range
        take_profit = round(take_profit_at, precision)

        expiration = self.get_expiration(symbol_info['time'])
        symbol = self._settings.symbol
        return OrderWrapper(
            order_type=order_type,
            order_mode=order_mode,
            price=price,
            symbol=symbol,
            expiration=expiration,
            stop_loss=stop_loss,
            take_profit=take_profit,
            volume=symbol_info['lotMin'],
            custom_comment=order_input['custom_comment']
        )

    def start(self):
        scenario_name = self.get_scenario_name()
        logger.info(f'Running EA for scenario: {scenario_name}')

        # Long shadowed candles
        strategy_settings = dict(
            symbol=self._settings.symbol,
            period=self._settings.period,
            candle_body_ratio=self._settings.candle_body_ratio,
            candle_height_quantile=self._settings.candle_height_quantile,
            regression_candles_count=self._settings.regression_candles_count,
            stop_loss_factor=self._settings.stop_loss_factor,
            take_profit_factor=self._settings.take_profit_factor
        )
        strategy = LongShadow(strategy_settings)

        ea_settings = ExpertAdvisorSettings(
            client=self._settings.client,
            symbol=self._settings.symbol,
            period=self._settings.period,
            scenario_name=scenario_name,
            run_at=self._settings.run_at
        )
        ea = ExpertAdvisor(ea_settings)
        raw_df = ea.from_api(drop_non_closed_candle=False)
        from_strategy_df = strategy.analyze(raw_df)

        last_row = from_strategy_df.iloc[-1]

        current_trades = ea.get_open_trades(scenario_name)
        if (len(current_trades) != 0):
            logger.info('Already placed orders')
            # self._settings.slack.send(f'{scenario_name}: {str(modification_resp)}')
        elif last_row['is_signal']:
            # just after required engulfing candles count is matched (1 candle ago)
            logger.info('Opposite orders placing')
            order_input = last_row.to_dict()
            order_input['custom_comment'] = scenario_name
            order_input['position_side'] = 'bearish' if last_row['beta'] > 0.0 else 'bullish'
            prepared_order = self.prepare_order(ea.get_symbol(), order_input)
            order_resp = ea.open_order_on_signal(prepared_order, ea.execute_tradeTransaction)
            logger.info(order_resp)
            self._settings.slack.send(f'{scenario_name}: {str(order_resp)}')
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
    parser.add_argument('-cbr', '--candle_body_ratio', type=float, required=True)
    parser.add_argument('-chq', '--candle_height_quantile', type=float, required=True)
    parser.add_argument('-rcc', '--regression_candles_count', type=int, required=True)
    parser.add_argument('-slf', '--stop_loss_factor', type=float, required=True)
    parser.add_argument('-tpf', '--take_profit_factor', type=float, required=True)

    args = parser.parse_args()
    user_id = os.getenv('XTB_API_USER')
    password = os.getenv('XTB_API_PASSWORD')
    symbol = args.symbol
    period = args.period
    candle_body_ratio = args.candle_body_ratio
    candle_height_quantile = args.candle_height_quantile
    regression_candles_count = args.regression_candles_count
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
            candle_body_ratio,
            candle_height_quantile,
            regression_candles_count,
            stop_loss_factor,
            take_profit_factor,
            run_at
        )
        EARunner(ea_runner_settings).start()

    finishing_at_string = pendulum.now(tz=local_tz).strftime("%d/%m/%Y %H:%M:%S")
    logger.info(f'Process ended at: {finishing_at_string}')

    client.commandExecute('logout')
    client.disconnect()
