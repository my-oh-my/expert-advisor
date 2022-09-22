import argparse
import os
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta

import pendulum
from xtb.wrapper.xtb_client import APIClient, loginCommand

from ea.messaging.slack import SlackService
from ea.misc.logger import logger
from ea.strategies.indicators.consolidation import Consolidation
from ea.strategies.indicators.waves import Waves
from ea.trading.expert_advisor import ExpertAdvisor, ExpertAdvisorSettings
from ea.trading.order import OrderWrapper, OrderType


@dataclass
class EARunnerSettings:
    client: APIClient
    slack: SlackService
    symbol: str
    period: int
    distance: float
    allowed_wave_percent_change: float
    waves_height_quantile: float
    minimum_waves_count: int
    trailing_sl: float
    run_at: datetime


class EARunner:
    def __init__(self, settings: EARunnerSettings):
        self._settings = settings

    def get_scenario_name(self):
        return f'symbol:{self._settings.symbol}-' \
               f'period:{self._settings.period}-' \
               f'distance:{self._settings.distance}-' \
               f'allowed_wave_percent_change:{self._settings.allowed_wave_percent_change}-' \
               f'waves_height_quantile:{self._settings.waves_height_quantile}-' \
               f'minimum_waves_count:{self._settings.minimum_waves_count}-' \
               f'trailing_sl:{self._settings.trailing_sl}'

    def time_mod(self, time, delta, epoch=None):
        if epoch is None:
            epoch = datetime(1970, 1, 1, tzinfo=time.tzinfo)
        return (time - epoch) % delta

    def time_floor(self, time, delta, epoch=None):
        mod = self.time_mod(time, delta, epoch)
        return time - mod

    def start(self):
        scenario_name = self.get_scenario_name()
        logger.info(f'Running EA for scenario: {scenario_name}')

        ea_settings = ExpertAdvisorSettings(
            client=self._settings.client,
            symbol=self._settings.symbol,
            period=self._settings.period,
            scenario_name=scenario_name,
            run_at=self._settings.run_at
        )
        ea = ExpertAdvisor(ea_settings)
        raw_dataframe = ea.from_api(drop_non_closed_candle=False)

        # Waves
        waves_settings = dict(symbol=self._settings.symbol, period=self._settings.period,
                              distance=self._settings.distance)
        waves_strategy = Waves(waves_settings)

        waves_df = waves_strategy.analyze(raw_dataframe)

        # Consolidation
        trailing_sl = self._settings.trailing_sl
        consolidation_settings = dict(
            symbol=self._settings.symbol,
            period=self._settings.period,
            allowed_wave_percent_change=self._settings.allowed_wave_percent_change,
            waves_height_quantile=self._settings.waves_height_quantile,
            minimum_waves_count=self._settings.minimum_waves_count,
            trailing_sl=trailing_sl
        )
        consolidation_strategy = Consolidation(consolidation_settings)
        consolidation_df = consolidation_strategy.analyze(waves_df)

        # single open position at a time
        last_open_position_at = consolidation_df['open_position_at'].max()
        since_last_open_position = consolidation_df[(consolidation_df['date_time'] >= last_open_position_at)]

        current_trade = ea.get_open_trade(scenario_name)
        if current_trade is not None:
            logger.info(f'Order modification')
            current_trade_open_time = datetime.fromtimestamp(int(current_trade['open_time'] / 1000))
            current_trade_open_candle = self.time_floor(current_trade_open_time, timedelta(minutes=self._settings.period))
            since_current_trade_open_candle = consolidation_df[(consolidation_df['date_time'] >= current_trade_open_candle)]

            current_stop_loss = current_trade['sl']
            current_trade_market = ea.get_current_trade_market(current_trade['cmd'])

            calculated_stop_loss = max([since_current_trade_open_candle['high'].max() - trailing_sl, current_stop_loss])  \
                if current_trade_market == 'bullish' \
                else min([since_current_trade_open_candle['low'].min() + trailing_sl, current_stop_loss])
            candidate_stop_loss = round(calculated_stop_loss, current_trade['digits'])
            if current_stop_loss != candidate_stop_loss:
                logger.info(f'Modifying order with SL at: {candidate_stop_loss}')
                modified_order = OrderWrapper(
                    order_mode=current_trade['cmd'],
                    price=current_trade['open_price'],
                    symbol=current_trade['symbol'],
                    order_type=OrderType.MODIFY.value,
                    expiration=current_trade['expiration'],
                    order_number=current_trade['order'],
                    stop_loss=candidate_stop_loss,
                    take_profit=current_trade['tp'],
                    volume=current_trade['volume'],
                    custom_comment=current_trade['customComment']
                )
                modification_resp = ea.modifyPosition(modified_order)
                logger.info(modification_resp)
                self._settings.slack.send(f'{scenario_name}: {str(modification_resp)}')
        elif len(since_last_open_position) == 1:
            logger.info(f'Order opening')
            order_input = since_last_open_position.iloc[0].to_dict()
            order_input['custom_comment'] = scenario_name
            order_resp = ea.open_order_on_signal(order_input, ea.prepare_order, ea.execute_tradeTransaction)
            logger.info(order_resp)
            self._settings.slack.send(f'{scenario_name}: {str(order_resp)}')
        else:
            logger.info(f'No signal')


if __name__ == "__main__":
    local_tz = pendulum.timezone('Europe/Warsaw')
    run_at = pendulum.now(tz=local_tz)
    running_at_string = run_at.strftime("%d/%m/%Y %H:%M:%S")
    logger.info(f'Process started at: {running_at_string}')

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--symbol', type=str, required=True)
    parser.add_argument('-pd', '--period', type=int, required=True)
    parser.add_argument('-d', '--distance', type=float, required=True)
    parser.add_argument('-ap', '--allowed_wave_percent_change', type=float, required=True)
    parser.add_argument('-wh', '--waves_height_quantile', type=float, required=True)
    parser.add_argument('-wc', '--minimum_waves_count', type=int, required=True)
    parser.add_argument('-tl', '--trailing_sl', type=float, required=True)

    args = parser.parse_args()
    user_id = os.getenv('XTB_API_USER')
    password = os.getenv('XTB_API_PASSWORD')
    symbol = args.symbol
    period = args.period
    distance = args.distance
    allowed_wave_percent_change = args.allowed_wave_percent_change
    waves_height_quantile = args.waves_height_quantile
    minimum_waves_count = args.minimum_waves_count
    trailing_sl = args.trailing_sl

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
            distance,
            allowed_wave_percent_change,
            waves_height_quantile,
            minimum_waves_count,
            trailing_sl,
            run_at
        )
        EARunner(ea_runner_settings).start()

    finishing_at_string = pendulum.now(tz=local_tz).strftime("%d/%m/%Y %H:%M:%S")
    logger.info(f'Process ended at: {finishing_at_string}')

    client.commandExecute('logout')
    client.disconnect()
