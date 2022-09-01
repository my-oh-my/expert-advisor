import argparse
from dataclasses import dataclass
from datetime import datetime

import pendulum
from xtb.wrapper.xtb_client import APIClient, loginCommand

from ea.misc.logger import logger
from ea.strategies.indicators.consolidation import Consolidation
from ea.strategies.indicators.waves import Waves
from ea.trading.expert_advisor import ExpertAdvisor, ExpertAdvisorSettings
from ea.trading.order import OrderWrapper, OrderType


@dataclass
class EARunnerSettings:
    client: APIClient
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

    def start(self):
        ea_settings = ExpertAdvisorSettings(
            client=self._settings.client,
            symbol=self._settings.symbol,
            period=self._settings.period,
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
        allowed_wave_percent_change = self._settings.allowed_wave_percent_change
        waves_height_quantile = self._settings.waves_height_quantile
        minimum_waves_count = self._settings.minimum_waves_count
        trailing_sl = self._settings.trailing_sl
        consolidation_settings = dict(
            symbol=self._settings.symbol,
            period=self._settings.period,
            allowed_wave_percent_change=allowed_wave_percent_change,
            waves_height_quantile=waves_height_quantile,
            minimum_waves_count=minimum_waves_count,
            trailing_sl=trailing_sl
        )
        consolidation_strategy = Consolidation(consolidation_settings)
        consolidation_df = consolidation_strategy.analyze(waves_df)

        # single open position at a time
        last_open_position_at = consolidation_df['open_position_at'].max()
        since_last_open_position = consolidation_df[(consolidation_df['date_time'] >= last_open_position_at)]

        current_trade = ea.get_open_trade()
        if current_trade is not None:
            logger.info(f'Order modification')
            current_stop_loss = current_trade['sl']
            current_trade_market = ea.get_current_trade_market(current_trade['cmd'])
            calculated_stop_loss = max([since_last_open_position['high'].max() - trailing_sl, current_stop_loss])  \
                if current_trade_market == 'bullish' \
                else min([since_last_open_position['low'].min() + trailing_sl, current_stop_loss])
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
                    stop_loss=candidate_stop_loss
                )
                logger.info(ea.modifyPosition(modified_order))
        elif len(since_last_open_position) == 1:
            logger.info(f'Order opening')
            order_input = since_last_open_position.iloc[0].to_dict()
            logger.info(ea.open_order_on_signal(order_input, ea.prepare_order, ea.execute_tradeTransaction))
        else:
            logger.info(f'No signal')


if __name__ == "__main__":
    local_tz = pendulum.timezone('Europe/Warsaw')
    run_at = pendulum.now(tz=local_tz)
    running_at_string = run_at.strftime("%d/%m/%Y %H:%M:%S")
    logger.info(f'Process started at: {running_at_string}')

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user_id', type=int, required=True)
    parser.add_argument('-p', '--password', type=str, required=True)
    parser.add_argument('-s', '--symbol', type=str, required=True)
    parser.add_argument('-pd', '--period', type=int, required=True)
    parser.add_argument('-d', '--distance', type=float, required=True)
    parser.add_argument('-ap', '--allowed_wave_percent_change', type=float, required=True)
    parser.add_argument('-wh', '--waves_height_quantile', type=float, required=True)
    parser.add_argument('-wc', '--minimum_waves_count', type=int, required=True)
    parser.add_argument('-tl', '--trailing_sl', type=float, required=True)

    args = parser.parse_args()
    user_id = args.user_id
    password = args.password
    symbol = args.symbol
    period = args.period
    distance = args.distance
    allowed_wave_percent_change = args.allowed_wave_percent_change
    waves_height_quantile = args.waves_height_quantile
    minimum_waves_count = args.minimum_waves_count
    trailing_sl = args.trailing_sl

    # symbol = 'W20'
    # period = 30
    # distance = 200
    # allowed_wave_percent_change = 2.0
    # waves_height_quantile = 0.9
    # minimum_waves_count = 5
    # trailing_sl = 60.0
    #
    client = APIClient()
    loginResponse = client.execute(loginCommand(userId=user_id, password=password))

    if not loginResponse['status']:
        logger.error('Login failed. Error code: {0}'.format(loginResponse['errorCode']))
    else:
        scenario_name = f'symbol:{symbol}-' \
                        f'period:{period}-' \
                        f'distance:{distance}-' \
                        f'allowed_wave_percent_change:{allowed_wave_percent_change}-' \
                        f'waves_height_quantile:{waves_height_quantile}-' \
                        f'minimum_waves_count:{minimum_waves_count}-' \
                        f'trailing_sl:{trailing_sl}'
        logger.info(f'Running EA for scenario: {scenario_name}')
        ea_runner_settings = EARunnerSettings(
            client,
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
