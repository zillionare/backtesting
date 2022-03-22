import datetime
import uuid
from enum import IntEnum


class EntrustSide(IntEnum):
    BUY = 1
    SELL = -1

    def __str__(self):
        return {EntrustSide.BUY: "买入", EntrustSide.SELL: "卖出"}[self]


class BidType(IntEnum):
    LIMIT = 1
    MARKET = 2

    def __str__(self):
        return {
            BidType.LIMIT: "限价委托",
            BidType.MARKET: "市价委托",
        }.get(self)


class Entrust:
    def __init__(
        self,
        security: str,
        side: EntrustSide,
        shares: int,
        price: float,
        bid_time: datetime.datetime,
        bid_type: BidType = BidType.MARKET,
    ):
        self.eid = str(uuid.uuid4())  # the contract id
        self.security = security
        self.side = side
        self.bid_type = bid_type
        self.bid_shares = shares
        self.bid_price = price
        self.bid_time = bid_time

    def to_json(self):
        return {
            "eid": self.eid,
            "security": self.security,
            "side": str(self.side),
            "bid_shares": self.bid_shares,
            "bid_price": self.bid_price,
            "bid_type": str(self.bid_type),
            "bid_time": self.bid_time.isoformat(),
        }


class EntrustError(IntEnum):
    SUCCESS = 0
    PARTIAL_SUCCESS = 1
    GENERIC_ERROR = -1
    NO_CASH = -2
    REACH_BUY_LIMIT = -3
    REACH_SELL_LIMIT = -4
    NO_POSITION = -5

    def __str__(self):
        return {
            EntrustError.SUCCESS: "成功委托",
            EntrustError.PARTIAL_SUCCESS: "部成",
            EntrustError.GENERIC_ERROR: "委托失败",
            EntrustError.NO_CASH: "资金不足",
            EntrustError.REACH_BUY_LIMIT: "不能在涨停板上买入",
            EntrustError.REACH_SELL_LIMIT: "不能在跌停板上卖出",
            EntrustError.NO_POSITION: "没有持仓",
        }.get(self)


position_dtype = [
    ("security", "O"),
    ("shares", "<f8"),
    ("sellable", "<f8"),
    ("price", "<f8"),
]
