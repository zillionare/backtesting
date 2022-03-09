import uuid
from enum import IntEnum


class OrderSide(IntEnum):
    BUY = 1
    SELL = -1


class BidType(IntEnum):
    LIMIT = 1
    MARKET = 2


class Order:
    def __init__(
        self,
        security: str,
        side: OrderSide,
        volume: int,
        price: float = None,
        bid_type: BidType = BidType.MARKET,
    ):
        self.security = security
        self.side = side
        self.volume = volume
        self.price = price
        self.bid_type = bid_type

        self.oid = uuid.uuid4()


class Trade:
    def __init__(self, order: Order, price: float, volume: int):
        self.order = order
        self.price = price
        self.volume = volume
        self.tid = uuid.uuid4()
