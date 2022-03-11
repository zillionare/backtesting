import datetime
from collections import defaultdict
from typing import Dict, Tuple

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
        self.available_cash = cash  # 当前可用资金
        self.assets = {}  # 每日总资产, 包括本金和持仓资产

        self.positions = {}

        self.trades = []
        self.orders = []
        self.transactions = []

    @property
    def current_assets(self):
        days = sorted(list(self.assets.keys()))

        if len(days) == 0:
            return self.cash

        return self.assets[days[-1]]

    def __str__(self):
        s = (
            f"账户：{self.account_name}:\n"
            + f"    总资产：{self.current_assets}\n"
            + f"    本金：{self.cash}\n"
            + f"    可用资金：{self.available_cash}\n"
            + f"    持仓：{','.join(self.positions.keys())}\n"
        )

        return s

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>{self}"

    async def buy(
        self,
        security: str,
        price: float,
        shares_asked: int,
        order_time: datetime.datetime,
        request_id: str = None,
    ) -> Dict:
        """买入委托

        买入以尽可能实现委托为目标。如果可用资金不足，但能买入部分股票，则部分买入。

        Args:
            security : 证券代码
            price : 委托价格。如果为None，则为市价委托
            shares_asked : 询买的股数
            order_time: 委托时间
            request_id: 请求ID

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

        # 获取用以撮合的数据
        bars = await feed.get_bars(security, now)

        # 排除在涨停板上买入的情况
        now_price = math_round(bars[bars["frame"] == order_time]["close"], 2)
        if now_price == math_round(buy_limit_price, 2):
            return make_response(EntrustError.REACH_BUY_LIMIT)

        # 将买入数限制在可用资金范围内
        shares_to_buy = min(
            shares_asked, self.available_cash // (price * (1 + self.commission))
        )

        # 必须以手为单位买入，否则委托会失败
        shares_to_buy = shares_to_buy // 100 * 100
        if shares_to_buy < 100:
            return make_response(EntrustError.NO_CASH)

        bars = feed.remove_buy_limit_bars(bars, buy_limit_price)

        mean_price, filled = self._calc_price_and_volume(bars, shares_to_buy)

        order = Order(
            request_id,
            security,
            OrderSide.BUY,
            shares_asked,
            price,
            order_time,
            bid_type,
        )

        return await self._fill_buy_order(order, mean_price, filled)

    def _calc_price_and_volume(self, bids, shares_to_buy) -> Tuple[float, float]:
        """计算此次买入的成交均价和成交量

        Args:
            bids : 提供撮合数据
            shares_to_buy : 要买入的股数

        Returns:
            成交均价和可埋单股数
        """
        c, v = bids["close"], bids["volume"]

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

        return mean_price, filled

    async def _fill_buy_order(self, order: Order, price: float, filled: float):
        self.orders.append(order)

        money = price * filled
        fee = money * self.commission

        trade = Trade(order, price, filled, fee)
        self._merge_position(trade)
        self.trades.append(trade)

        msg = "委托成功"
        if filled < order.shares:
            status = EntrustError.PARTIAL_SUCCESS
            msg += "，部分成交{}股".format(filled)
        else:
            status = EntrustError.SUCCESS

        self.available_cash -= money + fee

        # 当发生新的买入时，更新资产
        await self._update_assets(order.order_time.date())

        return make_response(status, data=trade, err_msg=msg)

    async def _update_assets(self, date: datetime.date):
        """更新当前资产

        在每次资产变动时进行计算和更新。

        Args:
            date: 当前资产（持仓）所属于的日期
        """
        market_value = 0
        feed = get_app_context().feed
        closes = await feed.get_close_price(self.positions.keys(), date)
        for sec, _, shares in self.positions.values():
            market_value += closes[sec] * shares

        self.assets[date] = self.available_cash + market_value

    def _merge_position(self, trade: Trade):
        """将成交合并到已有的持仓中

        持仓数据包括：证券代码、平均成本和持仓量。
        """
        sec = trade.security

        if sec not in self.positions:
            self.positions[sec] = [trade.security, trade.price, trade.shares]
        else:
            old = self.positions[sec]

            shares = trade.shares + old.shares
            price = (old.price * old.shares + trade.price * trade.shares) / shares

            self.positions[sec] = [sec, price, shares]

    def _fill_sell_order(self, order: Order, price: float, filled: float) -> Dict:
        """从positions中扣减股票、增加可用现金

        Args:
            order : 委卖单
            price : 成交均价
            filled : 回报的卖出数量

        Returns:
            response,格式参考make_response
        """
        money = price * filled
        fee = money * self.commission

        security = order.security
        if security not in self.positions:
            return make_response(EntrustError.NO_POSITION)

        for position in self.positions[security]:  # T + 1 sell
            if position.order_time.date() < order.order_time.date():
                closed_shares = min(position.shares, filled)
                money = closed_shares * price
                fee = money * self.commission
                trade = Trade(order, price, closed_shares, fee)
                self.trades[trade.tid] = trade

                self.cash_flow += money - fee

                filled -= closed_shares
                if filled == 0:
                    return make_response(EntrustError.SUCCESS, data=trade)

        if filled > 0:
            return make_response(EntrustError.PARTIAL_SUCCESS)

    async def sell(
        self,
        security: str,
        price: float,
        shares_asked: int,
        order_time: datetime.datetime,
        request_id: str = None,
    ) -> Dict:
        """卖出委托

        Args:
            security : 委托证券代码
            price : 出售价格，如果为None，则为市价委托
            shares_asked : 询卖股数
            order_time: 委托时间
            request_id: 请求ID

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
        _, _, sell_limit_price = (
            await Stock.get_trade_price_limits(security, now, now)
        )[0]

        if price is None:
            bid_type = BidType.MARKET
            price = sell_limit_price
        else:
            bid_type = BidType.LIMIT

        # fill the order, get mean price
        bars = await feed.get_bars(security, now)

        now_price = math_round(bars[bars["frame"] == order_time]["close"], 2)
        if now_price == math_round(sell_limit_price, 2):
            return make_response(EntrustError.REACH_SELL_LIMIT)

        bars = feed.remove_sell_limit_bars(bars, sell_limit_price)

        c, v = bars["close"], bars["volume"]

        cum_v = np.cumsum(v)

        shares_to_sell = self._get_sellable_shares(security, shares_asked, order_time)
        # until i the order can be filled
        where_total_filled = np.argwhere(cum_v >= shares_to_sell)
        if len(where_total_filled) == 0:
            i = len(v) - 1
        else:
            i = np.min(where_total_filled)

        # 也许到当天结束，都没有足够的股票
        filled = min(cum_v[i], shares_to_sell)

        # 最后一周期，只需要成交剩余的部分
        vol = v[: i + 1].copy()
        vol[-1] = filled - np.sum(vol[:-1])

        money = sum(c[: i + 1] * vol)
        mean_price = money / filled

        order = Order(
            request_id,
            security,
            OrderSide.SELL,
            shares_asked,
            price,
            order_time,
            bid_type,
        )

        return self._fill_sell_order(order, mean_price, filled)

    def _get_sellable_shares(
        self, security: str, shares_asked: int, order_time: datetime.datetime
    ) -> int:
        """获取可卖股数

        Args:
            security: 证券代码

        Returns:
            可卖股数
        """
        if security not in self.positions:
            return 0

        shares = 0
        for position in self.positions[security]:
            if position.order_time.date() < order_time.date():
                shares += position.shares

        return min(shares_asked, shares)
