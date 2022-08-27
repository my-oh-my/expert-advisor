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

    def get_expiration(self, current_time):
        waiting_candles_limit = 3
        time_long_to_wait = waiting_candles_limit * self.settings.period * 60 * 1000
        return current_time + time_long_to_wait

    def prepare_order(self, order_input: dict) -> OrderWrapper:
        get_symbol_resp = self.get_symbol()

        order_type = OrderType.OPEN.value
        order_mode = OrderMode.BUY_STOP.value if order_input['market'] == 'bullish' else OrderMode.SELL_STOP.value
        forward_price_factor = get_symbol_resp['tickSize'] * 10
        price = get_symbol_resp['ask'] + forward_price_factor \
            if order_input['market'] == 'bullish' \
            else get_symbol_resp['bid'] - forward_price_factor

        symbol = self.settings.symbol
        stop_loss = round(order_input['recent_consolidation_mid'], get_symbol_resp['precision'])

        expiration = self.get_expiration(get_symbol_resp['time'])

        return OrderWrapper(
            order_type=order_type,
            order_mode=order_mode,
            price=price,
            symbol=symbol,
            expiration=expiration,
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

    def check_order_status(self, order: OrderWrapper, open_order_callable, attempt):
        logger.info(f"Attempting to open order, attempt no: {attempt}")
        order_number = open_order_callable(order)

        command_arguments = {"order": order_number}
        trade_transaction_status_resp = self.settings.client.commandExecute("tradeTransactionStatus", command_arguments)
        request_status = trade_transaction_status_resp['returnData']['requestStatus']
        next_attempt = attempt + 1
        if (request_status == 3) | (next_attempt == 4):
            return trade_transaction_status_resp
        else:
            self.check_order_status(order, open_order_callable, next_attempt)

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
        return self.check_order_status(prepare_order_callable(order_input), open_order_callable, 1)

    def get_candidate_stop_loss(self):
        pass

    def modifyPosition(self, order: OrderWrapper) -> int:
        return self.execute_tradeTransaction(order)

    def _closePosition(self, order: dict) -> int:
        pass

    def run(self):
        pass
