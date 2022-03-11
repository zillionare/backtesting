import datetime
import uuid
from enum import IntEnum


class OrderSide(IntEnum):
    BUY = 1
    SELL = -1

    def __str__(self):
        return {OrderSide.BUY: "买入", OrderSide.SELL: "卖出"}[self]


class BidType(IntEnum):
    LIMIT = 1
    MARKET = 2

    def __str__(self):
        return {
            BidType.LIMIT: "限价委托",
            BidType.MARKET: "市价委托",
        }.get(self)


class Order:
    def __init__(
        self,
        request_id: str,
        security: str,
        side: OrderSide,
        shares: int,
        price: float,
        order_time: datetime.datetime,
        bid_type: BidType = BidType.MARKET,
    ):
        self.request_id = request_id
        self.security = security
        self.side = side
        self.shares = shares
        self.price = price
        self.bid_type = bid_type
        self.order_time = order_time

    def to_json(self):
        return {
            "request_id": self.request_id,
            "security": self.security,
            "side": str(self.side),
            "shares": self.shares,
            "price": self.price,
            "bid_type": str(self.bid_type),
            "order_time": self.order_time.isoformat(),
        }


class Trade:
    def __init__(self, order: Order, price: float, shares: int, fee: float):
        self.__dict__.update(order.__dict__)

        self.price = price
        self.shares = shares
        self.fee = fee
        self.tid = uuid.uuid4()

    def __str__(self):
        return f"证券代码: {self.security}\n成交方向: {self.side}\n成交均价: {self.price}\n数量: {self.shares}\n手续费: {self.fee}\n委托号: {self.request_id}\n成交号: {self.tid}"

    def to_json(self):
        d = self.__dict__
        d["side"] = str(d["side"])
        d["bid_type"] = str(d["bid_type"])
        d["order_time"] = d["order_time"].isoformat()
        d["tid"] = str(d["tid"])

        return d


class EntrustError(IntEnum):
    SUCCESS = 0
    PARTIAL_SUCCESS = 1
    GENERIC_ERROR = -1
    NO_CASH = -2
    REACH_BUY_LIMIT = -3
    REACH_SELL_LIMIT = -4

    def __str__(self):
        return {
            EntrustError.SUCCESS: "成功委托",
            EntrustError.PARTIAL_SUCCESS: "部成",
            EntrustError.GENERIC_ERROR: "委托失败",
            EntrustError.NO_CASH: "资金不足",
            EntrustError.REACH_BUY_LIMIT: "不能在涨停板上买入",
            EntrustError.REACH_SELL_LIMIT: "不能在跌停板上卖出",
        }.get(self)
