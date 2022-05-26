import datetime
import uuid
from enum import IntEnum
from typing import Final

import numpy as np

E_BACKTEST: Final = "BACKTEST"


class EntrustSide(IntEnum):
    BUY = 1
    SELL = -1

    def __str__(self):
        return {EntrustSide.BUY: "买入", EntrustSide.SELL: "卖出"}[self]


class BidType(IntEnum):
    LIMIT = 1
    MARKET = 2

    def __str__(self):
        return {BidType.LIMIT: "限价委托", BidType.MARKET: "市价委托"}.get(self)


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


cash_dtype = np.dtype([("date", "O"), ("cash", "<f8")])

daily_position_dtype = np.dtype(
    [
        ("date", "O"),
        ("security", "O"),
        ("shares", "<f8"),
        ("sellable", "<f8"),
        ("price", "<f8"),
    ]
)

# for return to client
position_dtype = np.dtype(
    [
        ("security", "O"),
        ("shares", "<f8"),
        ("sellable", "<f8"),
        ("price", "<f8"),
    ]
)


assets_dtype = np.dtype([("date", "O"), ("assets", "<f8")])

float_ts_dtype = np.dtype([("date", "O"), ("value", "<f8")])
