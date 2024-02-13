from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta

from pandas import DataFrame
from xtb.wrapper.chart_last_request import ChartLastRequest
from xtb.wrapper.xtb_client import APIClient

from ea.misc.logger import logger
from ea.trading.backoff import retry
from ea.trading.exceptions import TransactionStatusException
from ea.trading.order import OrderMode, OrderType, OrderWrapper

import json


@dataclass
class ExpertAdvisorSettings:
    client: APIClient
    symbol: str
    period: int
    scenario_name: str
    run_at: datetime


class ExpertAdvisor:
    def __init__(self, settings: ExpertAdvisorSettings):
        self.settings = settings

    def process_non_closed_candle(self, dataframe: DataFrame, current_run_time: datetime) -> DataFrame:
        current_candle_open = dataframe.iloc[-1]['date_time']
        tz_current_candle_open = current_candle_open.tz_localize(tz='Europe/Warsaw')
        dt_current_candle_open = tz_current_candle_open.to_pydatetime(current_candle_open)
        dt_current_candle_end = dt_current_candle_open + timedelta(minutes=self.settings.period)

        return dataframe.iloc[:-1] if dt_current_candle_open < current_run_time < dt_current_candle_end else dataframe

    def initial_processing(self, dataframe: DataFrame, drop_non_closed_candle: bool) -> DataFrame:
        dataframe['date_time'] = dataframe["timestamp"].map(lambda value: datetime.fromtimestamp(int(value)))
        result = self.process_non_closed_candle(dataframe, self.settings.run_at) if drop_non_closed_candle else dataframe

        return result[['date_time', 'open', 'close', 'high', 'low', 'volume']]

    def from_api(self, drop_non_closed_candle: bool = True) -> DataFrame:
        raw_data = ChartLastRequest(self.settings.client)\
            .request_candle_history_with_limit(self.settings.symbol, self.settings.period)

        raw_dataframe = DataFrame(raw_data)

        return self.initial_processing(raw_dataframe, drop_non_closed_candle)

    @staticmethod
    def get_current_trade_market(cmd: int):
        return 'bullish' if cmd in [OrderMode.BUY.value, OrderMode.BUY_LIMIT.value, OrderMode.BUY_STOP.value] else 'bearish'

    def get_symbol(self):
        command_arguments = {"symbol": self.settings.symbol}
        get_symbol_resp = self.settings.client.commandExecute("getSymbol", command_arguments)
        return get_symbol_resp['returnData']

    def execute_tradeTransaction(self, order: OrderWrapper) -> dict:
        logger.info("Sending order!")
        logger.info(f'cmd/mode: {order.order_mode}')
        logger.info(f'symbol: {order.symbol}')
        logger.info(f'price: {order.price}')
        logger.info(f'stopLoss: {order.stop_loss}')
        logger.info(f'takeProfit: {order.take_profit}')
        logger.info(f'volume: {order.volume}')
        logger.info(f'orderNumber: {order.order_number}')
        logger.info(f'expiration: {order.expiration}')
        logger.info(f'customComment: {order.custom_comment}')

        command_arguments = order.get_tradeTransInfo_arguments()
        trade_transaction_resp = self.settings.client.commandExecute("tradeTransaction", command_arguments)
        logger.info(trade_transaction_resp)

        return trade_transaction_resp["returnData"]

    @retry(TransactionStatusException)
    def check_order_status(self, order: OrderWrapper, open_order_callable) -> dict:
        order_resp = open_order_callable(order)
        order_number = order_resp['order']

        command_arguments = {"order": order_number}
        trade_transaction_status_resp = self.settings.client.commandExecute("tradeTransactionStatus", command_arguments)
        request_status = trade_transaction_status_resp['returnData']['requestStatus']
        if request_status != 3:
            raise TransactionStatusException(trade_transaction_status_resp['returnData']['message'])

        return json.dumps(trade_transaction_status_resp, indent=2)

    def get_trades(self, opened_only: bool = True) -> list[dict]:
        command_arguments = {"openedOnly": opened_only}
        get_trades_resp = self.settings.client.commandExecute("getTrades", command_arguments)

        return get_trades_resp['returnData']

    def get_open_trades(self, scenario_name: str, opened_only=True):
        trades = self.get_trades(opened_only=opened_only)

        return list((item for item in trades if item["customComment"] == scenario_name))

    def open_order_on_signal(self, order: OrderWrapper, open_order_callable) -> dict:
        return self.check_order_status(order, open_order_callable)

    def modifyPosition(self, order: OrderWrapper) -> dict:
        return self.execute_tradeTransaction(order)
