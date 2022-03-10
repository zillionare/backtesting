import datetime
from typing import Dict

import arrow
import numpy as np
from omicron.extensions.decimals import math_round
from omicron.models.stock import Stock

from backtest.data.basefeed import BaseFeed
from backtest.helper import get_app_context, make_response
from backtest.types import BidType, EntrustError, Order, OrderSide, Trade


class Broker:
    def __init__(self, account_name: str, cash: float, commission: float):

        self.account_name = account_name
        self.commission = commission

        # 初始本金
        self.cash = cash
        self.available_cash = cash
        self.positions = {}
        self.trades = []
        self.orders = []

    async def buy(
        self,
        security: str,
        price: float,
        shares: int,
        order_time: datetime.datetime,
        request_id: str = None,
        timeout: int = None,
    ) -> Dict:
        """买入委托

        Args:
            security : _description_
            price : _description_
            shares : 买入股数
            timeout : _description_.

        Returns:
            {
                "status": 0 # 0表示成功，否则为错误码
                "msg": "blah"
                "data": {

                }
            }
        """
        feed = get_app_context().feed

        now = order_time.date()
        _, buy_limit_price, _ = (
            await Stock.get_trade_price_limits(security, now, now)
        )[0]

        if price is None:
            bid_type = BidType.MARKET
            price = buy_limit_price
        else:
            bid_type = BidType.LIMIT

        # fill the order, get mean price
        bars = await feed.get_bars(security, now)

        now_price = math_round(bars[bars["frame"] == order_time]["close"], 2)
        if now_price == math_round(buy_limit_price, 2):
            return make_response(EntrustError.REACH_BUY_LIMIT)

        shares_to_buy = min(
            shares, self.available_cash // (price * (1 + self.commission))
        )

        shares_to_buy = shares_to_buy // 100 * 100
        if shares_to_buy < 100:
            return make_response(EntrustError.FAILED_NOT_ENOUGH_CASH)

        bars = feed.remove_buy_limit_bars(bars, buy_limit_price)

        c, v = bars["close"], bars["volume"]

        cum_v = np.cumsum(v)

        # until i the order can be filled
        where_total_filled = np.argwhere(cum_v >= shares_to_buy)
        if len(where_total_filled) == 0:
            i = len(v) - 1
        else:
            i = np.min(where_total_filled)

        # 也许到当天结束，都没有足够的股票
        filled = min(cum_v[i], shares_to_buy)

        # 最后一周期，只需要成交剩余的部分
        vol = v[: i + 1].copy()
        vol[-1] = filled - np.sum(vol[:-1])

        money = sum(c[: i + 1] * vol)
        mean_price = money / filled

        order = Order(
            request_id,
            security,
            OrderSide.BUY,
            shares_to_buy,
            price,
            order_time,
            bid_type,
        )

        self.orders.append(order)

        fee = money * self.commission
        trade = Trade(order, mean_price, filled, fee)

        self._add_position(trade)

        if shares_to_buy < shares:
            msg = "资金余额不足，只能委托{}股".format(shares_to_buy)

        if filled < shares_to_buy:
            status = EntrustError.PARTIAL_SUCCESS
            msg += "，只能成交{}股".format(filled)
        else:
            status = EntrustError.SUCCESS
            msg = None

        return make_response(status, data=trade, err_msg=msg)

    def _add_position(self, trade: Trade):
        self.positions[trade.security] = trade
