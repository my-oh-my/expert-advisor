from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta

from pandas import DataFrame
from xtb.wrapper.chart_last_request import ChartLastRequest
from xtb.wrapper.xtb_client import APIClient

from ea.misc.logger import logger
from ea.trading.order import OrderMode, OrderType, OrderWrapper


@dataclass
class ExpertAdvisorSettings:
    client: APIClient
    symbol: str
    period: int
    run_at: datetime


class ExpertAdvisor:
    def __init__(self, settings: ExpertAdvisorSettings):
        self.settings = settings

    def process_non_closed_candle(self, dataframe: DataFrame, current_run_time: datetime) -> DataFrame:
        current_candle_open = dataframe.iloc[-1]['date_time']
        current_candle_end = current_candle_open + timedelta(minutes=self.settings.period)

        return dataframe.iloc[:-1] if current_candle_open < current_run_time < current_candle_end else dataframe

    def initial_processing(self, dataframe: DataFrame, drop_non_closed_candle: bool) -> DataFrame:
        dataframe['date_time'] = dataframe["timestamp"].map(lambda value: datetime.fromtimestamp(int(value)))
        result = dataframe if not drop_non_closed_candle else self.process_non_closed_candle(dataframe, self.settings.run_at)

        return result[['date_time', 'open', 'close', 'high', 'low', 'volume']]

    def from_api(self, drop_non_closed_candle: bool) -> DataFrame:
        raw_dataframe = ChartLastRequest(self.settings.client)\
            .collect_from_api(self.settings.symbol, self.settings.period)

        return self.initial_processing(raw_dataframe, drop_non_closed_candle)

    @staticmethod
    def get_current_trade_market(cmd: int):
        return 'bullish' if cmd in [OrderMode.BUY.value, OrderMode.BUY_LIMIT.value, OrderMode.BUY_STOP.value] else 'bearish'

    def prepare_order(self, order_input: dict) -> OrderWrapper:
        # order is about to be executed
        order_type = OrderType.OPEN.value
        order_mode = OrderMode.BUY.value if order_input['market'] == 'bullish' else OrderMode.SELL.value
        price = order_input['open_position_price_candidate']
        symbol = self.settings.symbol
        stop_loss = order_input['recent_consolidation_mid']

        return OrderWrapper(
            order_type=order_type,
            order_mode=order_mode,
            price=price,
            symbol=symbol,
            stop_loss=stop_loss
        )

    def execute_tradeTransaction(self, order: OrderWrapper) -> int:
        logger.info("Sending order!")
        logger.info(f"cmd/mode: {order.order_mode}")
        logger.info(f"symbol: {order.symbol}")
        logger.info(f"price: {order.price}")
        logger.info(f"stopLoss: {order.stop_loss}")
        logger.info(f"takeProfit: {order.take_profit}")
        logger.info(f"volume: {order.volume}")
        logger.info(f"orderNumber: {order.order_number}")
        logger.info(f"expiration: {order.expiration}")

        command_arguments = order.get_tradeTransInfo_arguments()
        trade_transaction_resp = self.settings.client.commandExecute("tradeTransaction", command_arguments)
        logger.info(trade_transaction_resp)

        return trade_transaction_resp["returnData"]["order"]

    def check_order_status(self, order: OrderWrapper, open_order_callable):
        order_number = open_order_callable(order)
        command_arguments = OrderWrapper.get_tradeTransactionStatus_arguments(order_number)

        return self.settings.client.commandExecute("tradeTransactionStatus", command_arguments)

    def get_symbol_trades(self, response: dict):
        return [trade for trade in response if trade['symbol'] == self.settings.symbol]

    def get_trades(self, opened_only: bool = True, symbol_only: bool = True) -> list[dict]:
        command_arguments = {"openedOnly": opened_only}
        get_trades_resp = self.settings.client.commandExecute("getTrades", command_arguments)
        return_data = get_trades_resp['returnData']
        result = self.get_symbol_trades(return_data) if symbol_only else return_data

        return result

    def get_open_trade(self):
        trades = self.get_trades(opened_only=True)

        return trades[0] if len(trades) != 0 else None

    def open_order_on_signal(self, order_input: dict, prepare_order_callable, open_order_callable):
        return self.check_order_status(prepare_order_callable(order_input), open_order_callable)

    def get_candidate_stop_loss(self):
        pass

    def modifyPosition(self, order: OrderWrapper) -> int:
        return self.execute_tradeTransaction(order)

    def _closePosition(self, order: dict) -> int:
        pass

    def run(self):
        pass
