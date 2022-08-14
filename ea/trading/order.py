from dataclasses import dataclass
from enum import Enum


class OrderMode(Enum):
    BUY = 0
    SELL = 1
    BUY_LIMIT = 2
    SELL_LIMIT = 3
    BUY_STOP = 4
    SELL_STOP = 5


class OrderType(Enum):
    OPEN = 0
    PENDING = 1
    CLOSE = 2
    MODIFY = 3
    DELETE = 4


class OrderStatus(Enum):
    ERROR = 0
    PENDING = 1
    ACCEPTED = 3
    REJECTED = 4


@dataclass
class Order:
    order_mode: int
    price: float
    symbol: str
    order_type: int = None
    expiration: int = 0
    offset: int = 0
    order_number: int = 0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    volume: float = 0.01
    position: int = None


class OrderWrapper(Order):
    def __init__(self,
                 order_mode: int,
                 price: float,
                 symbol: str,
                 order_type: int = None,
                 expiration: int = 0,
                 offset: int = 0,
                 order_number: int = 0,
                 stop_loss: float = 0.0,
                 take_profit: float = 0.0,
                 volume: float = 0.01,
                 position: int = None):
        self.order_type = order_type
        self.order_mode = order_mode
        self.price = price
        self.symbol = symbol
        self.expiration = expiration
        self.offset = offset
        self.order_number = order_number
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.volume = volume
        self.position = position

    def get_tradeTransInfo_arguments(self) -> dict:
        return {
            "tradeTransInfo": {
                "cmd": self.order_mode,
                "customComment": "Test",
                "expiration": self.expiration,
                "offset": self.offset,
                "order": self.order_number,
                "price": self.price,
                "sl": self.stop_loss,
                "symbol": self.symbol,
                "tp": self.take_profit,
                "type": self.order_type,
                "volume": self.volume
            }
        }

    @staticmethod
    def get_tradeTransactionStatus_arguments(order_number: int) -> dict:
        return {"order": order_number}
