import datetime
import logging
from typing import Dict, List, Tuple

import numpy as np
from coretypes import FrameType
from omicron.extensions.np import math_round
from omicron.models.timeframe import TimeFrame as tf

from backtest.common.errors import BadParameterError
from backtest.common.helper import get_app_context, make_response
from backtest.trade.trade import Trade
from backtest.trade.types import (
    BidType,
    Entrust,
    EntrustError,
    EntrustSide,
    position_dtype,
)

logger = logging.getLogger(__name__)


class Broker:
    def __init__(self, account_name: str, capital: float, commission: float):
        """_summary_

        Args:
            account_name : 账号/策略名
            capital : 初始本金
            commission : 佣金率
        """

        self.account_name = account_name
        self.commission = commission

        # 初始本金
        self.capital = capital
        self.cash = capital  # 当前可用资金
        self._assets = {}  # 每日总资产, 包括本金和持仓资产
        self._unclosed_trades = {}  # 未平仓的交易

        # 委托列表，包括废单和未成交委托
        self.entrusts = {}

        # 所有的成交列表，包括买入和卖出，已关闭和未关闭的
        self.trades = {}

        # trasaction = buy + sell trade
        self.transactions = []

    def get_unclosed_trades(self, dt: datetime.date) -> set:
        """获取`dt`当天未平仓的交易

        如果`dt`小于首次交易日，则返回空，否则，如果当日无数据，将从上一个有数据之日起，进行补齐填充。
        """
        if len(self._unclosed_trades) == 0:
            return set()

        result = self._unclosed_trades.get(dt)
        if result is None:
            start = sorted(self._unclosed_trades.keys())[0]
            if dt < start:
                return set()
            else:
                self._fillup_unclosed_trades(dt)

        return self._unclosed_trades.get(dt)

    def get_positions(self, dt: datetime.date) -> List:
        """获取`dt`日持仓

        Args:
            dt : 查询哪一天的持仓

        Returns:
            返回结果为[(security, heldings, sellable, price)]，其中price为该批持仓的均价。
        """
        unclosed = self.get_unclosed_trades(dt)

        positions = {}
        for tid in unclosed:
            trade = self.trades[tid]
            sec = trade.security

            assert trade.side == EntrustSide.BUY
            assert trade._unsell > 0
            assert trade.closed is False

            position = positions.get(sec)

            if position is None:
                sellable = trade._unsell if trade.time.date() < dt else 0
                positions[sec] = (trade._unsell, sellable, trade.price)
            else:
                shares, sellable, price = position
                price = (price * shares + trade.price * trade._unsell) / (
                    shares + trade._unsell
                )
                shares += trade._unsell

                if trade.time.date() < dt:
                    sellable += trade._unsell

                positions[sec] = (shares, sellable, price)

        return np.array(
            [
                (sec, heldings, sellable, price)
                for sec, (heldings, sellable, price) in positions.items()
            ],
            dtype=position_dtype,
        )

    @property
    def info(self) -> Dict:
        """账号相关信息

        Returns:
            A dict of the following:
            ‒ start: since when the account is set
            ‒ name: the name/id of the account
            ‒ assets: 当前资产
            ‒ captial: 本金
            ‒ last_trade: 最后一笔交易时间
            ‒ 交易笔数
        """
        sorted_days = sorted(list(self._assets.keys()))

        if len(sorted_days) > 0:
            start, end = sorted_days[0], sorted_days[-1]
            start = start.isoformat()
            end = end.isoformat()
        else:
            start, end = None, None
        return {
            "start": start,
            "name": self.account_name,
            "assets": self.assets,
            "capital": self.capital,
            "last_trade": None,
            "trade_count": len(self.transactions),
            "trades": self.transactions,
            "earnings": self.assets - self.capital,
            "returns": self.get_returns().tolist(),
        }

    def get_returns(self, date: datetime.date = None) -> List[float]:
        """求截止`date`时的每日回报

        Args:
            date : _description_.

        Returns:
            _description_
        """
        dtype = [("date", "O"), ("assets", "f4")]
        assets = np.array(
            [(d, self._assets[d]) for d in sorted(self._assets.keys())], dtype=dtype
        )

        if date is not None:
            assets = assets[assets["date"] <= date]

        returns = [self.capital] + assets["assets"]

        return np.diff(returns) / returns[:-1]

    @property
    def assets(self) -> float:
        """当前总资产。

        如果要获取历史上某天的总资产，请使用`get_assets`方法。
        """
        days = sorted(list(self._assets.keys()))

        if len(days) == 0:
            return self.capital

        return self._assets[days[-1]]

    def get_assets(self, date: datetime.date) -> float:
        days = sorted(list(self._assets.keys()))

        if len(days) == 0 or date < days[0]:
            return self.capital

        pos = np.argwhere(date >= days)[0]

        dt = days[pos]
        return self._assets[dt]

    @property
    def positions(self):
        """获取当前持仓

        如果要获取历史上某天的持仓，请使用`get_positions`方法。

        Returns:
            返回成员为(security, shares, price)的numpy structure array
        """
        days = sorted(self._unclosed_trades.keys())

        if len(days) == 0:
            return np.array([], dtype=position_dtype)

        return self.get_positions(days[-1])

    def __str__(self):
        s = (
            f"账户：{self.account_name}:\n"
            + f"    总资产：{self.assets:,.2f}\n"
            + f"    本金：{self.capital:,.2f}\n"
            + f"    可用资金：{self.cash:,.2f}\n"
            + f"    持仓：{self.positions}\n"
        )

        return s

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>{self}"

    async def buy(
        self,
        security: str,
        bid_price: float,
        bid_shares: int,
        bid_time: datetime.datetime,
    ) -> Dict:
        """买入委托

        买入以尽可能实现委托为目标。如果可用资金不足，但能买入部分股票，则部分买入。

        Args:
            security : 证券代码
            bid_price : 委托价格。如果为None，则为市价委托
            bid_shares : 询买的股数
            bid_time: 委托时间
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

        en = Entrust(
            security,
            EntrustSide.BUY,
            bid_shares,
            bid_price,
            bid_time,
            BidType.LIMIT if bid_price is not None else BidType.MARKET,
        )

        logger.info(
            "买入委托(%s): %s %d %.2f, 单号：%s",
            bid_time,
            security,
            bid_shares,
            bid_price,
            en.eid,
        )
        self.entrusts[en.eid] = en

        _, buy_limit_price, _ = await feed.get_trade_price_limits(
            security, bid_time.date()
        )

        bid_price = bid_price or buy_limit_price

        # 获取用以撮合的数据
        bars = await feed.get_bars_for_match(security, bid_time)

        # 排除在涨停板上买入的情况
        if self._reached_trade_price_limits(bars, bid_time, buy_limit_price):
            logger.info("撮合失败: %s(%s)挂单时已达到涨停板", security, en.eid)
            return make_response(EntrustError.REACH_BUY_LIMIT)

        # 将买入数限制在可用资金范围内
        shares_to_buy = min(
            bid_shares, self.cash // (bid_price * (1 + self.commission))
        )

        # 必须以手为单位买入，否则委托会失败
        shares_to_buy = shares_to_buy // 100 * 100
        if shares_to_buy < 100:
            logger.info("委买失败：%s(%s), 资金(%s)不足", security, self.cash, en.eid)
            return make_response(EntrustError.NO_CASH)

        bars = self._remove_for_buy(bars, bid_price, buy_limit_price)

        mean_price, filled, close_time = self._match_buy(bars, shares_to_buy)

        return await self._fill_buy_order(en, mean_price, filled, close_time)

    def _match_buy(
        self, bid_queue, shares_to_buy
    ) -> Tuple[float, float, datetime.datetime]:
        """计算此次买入的成交均价和成交量

        Args:
            bid_queue : 撮合数据
            shares_to_buy : 要买入的股数

        Returns:
            成交均价、可埋单股数和最后成交时间
        """
        c, v = bid_queue["close"], bid_queue["volume"]

        cum_v = np.cumsum(v)

        # until i the order can be filled
        where_total_filled = np.argwhere(cum_v >= shares_to_buy)
        if len(where_total_filled) == 0:
            i = len(v) - 1
        else:
            i = np.min(where_total_filled)

        # 也许到当天结束，都没有足够的股票
        filled = min(cum_v[i], shares_to_buy) // 100 * 100

        # 最后一周期，只需要成交剩余的部分
        vol = v[: i + 1].copy()
        vol[-1] = filled - np.sum(vol[:-1])

        money = sum(c[: i + 1] * vol)
        mean_price = money / filled

        return mean_price, filled, bid_queue["frame"][i]

    def _fillup_unclosed_trades(self, dt: datetime.date):
        if len(self._unclosed_trades) != 0 and self._unclosed_trades.get(dt) is None:
            days = sorted(list(self._unclosed_trades.keys()))
            frames = tf.get_frames(days[-1], dt, FrameType.DAY)
            for src, dst in zip(frames[:-1], frames[1:]):
                src = tf.int2date(src)
                dst = tf.int2date(dst)
                self._unclosed_trades[dst] = self._unclosed_trades[src].copy()

    def _append_unclosed_trades(self, tid, date: datetime.date):
        """记录每日持有的未平仓交易

        Args:
            trades: 交易列表
        """
        unclosed = self._unclosed_trades.get(date, [])
        if len(unclosed):
            unclosed.append(tid)

            return

        if len(self._unclosed_trades) == 0:
            self._unclosed_trades[date] = [tid]
            return

        # 记录还未创建，需要复制前一日记录
        self._fillup_unclosed_trades(date)

        self._unclosed_trades[date].append(tid)

    async def _fill_buy_order(
        self, en: Entrust, price: float, filled: float, close_time: datetime.datetime
    ):
        money = price * filled
        fee = money * self.commission

        trade = Trade(en.eid, en.security, price, filled, fee, en.side, close_time)
        self.trades[trade.tid] = trade
        self._append_unclosed_trades(trade.tid, close_time.date())

        logger.info(
            "买入成交(%s): %s (%d %.2f %.2f),委单号: %s, 成交号: %s",
            close_time,
            en.security,
            filled,
            price,
            fee,
            en.eid,
            trade.tid,
        )

        # 当发生新的买入时，更新资产
        cash_change = -1 * (money + fee)
        await self._update_assets(cash_change, close_time.date())

        msg = "委托成功"
        if filled < en.bid_shares:
            status = EntrustError.PARTIAL_SUCCESS
            msg += "，部分成交{}股".format(filled)
        else:
            status = EntrustError.SUCCESS

        return make_response(status, data=trade.to_json(), err_msg=msg)

    async def _update_assets(self, cash_change: float, date: datetime.date):
        """更新当前资产（含持仓）

        在每次资产变动时进行计算和更新。

        Args:
            cash_change : 变动的现金
            date: 当前资产（持仓）所属于的日期
        """
        self.cash += cash_change

        positions = self.get_positions(date)

        held_secs = [pos[0] for pos in positions]

        market_value = 0
        feed = get_app_context().feed
        closes = await feed.get_close_price(held_secs, date)

        for sec, shares, sellable, _ in positions:
            market_value += closes[sec] * shares

        self._assets[date] = self.cash + market_value
        logger.info(
            f"资产更新({date})<总资产:{self.assets:,.2f} 可用:{self.cash:,.2f} 本金:{self.capital:,.2f}>"
        )
        logger.info("持仓: \n%s", positions)

    async def _fill_sell_order(self, en: Entrust, price: float, to_sell: float) -> Dict:
        """从positions中扣减股票、增加可用现金

        Args:
            en : 委卖单
            price : 成交均价
            filled : 回报的卖出数量

        Returns:
            response,格式参考make_response
        """
        dt = en.bid_time.date()

        money = price * to_sell
        fee = money * self.commission

        security = en.security

        unclosed_trades = self.get_unclosed_trades(dt)
        closed_trades = []
        exit_trades = []
        refund = 0
        while to_sell > 0:
            for tid in unclosed_trades:
                trade = self.trades[tid]
                if trade.security != security:
                    continue

                if trade.time.date() >= dt:
                    # not T + 1
                    continue

                to_sell, fee, exit_trade, tx = trade.sell(
                    to_sell, price, fee, en.bid_time
                )

                logger.info(
                    "卖出成交(%s): %s (%d %.2f %.2f),委单号: %s, 成交号: %s",
                    exit_trade.time,
                    en.security,
                    exit_trade.shares,
                    exit_trade.price,
                    exit_trade.fee,
                    en.eid,
                    exit_trade.tid,
                )
                exit_trades.append(exit_trade)
                self.trades[exit_trade.tid] = exit_trade
                self.transactions.append(tx)

                refund += exit_trade.shares * exit_trade.price

                if trade.closed:
                    closed_trades.append(tid)
            else:  # no more unclosed trades, even if to_sell > 0
                break

        unclosed_trades = [tid for tid in unclosed_trades if tid not in closed_trades]
        self._unclosed_trades[dt] = unclosed_trades

        # 扣除卖出费用
        refund -= fee
        await self._update_assets(refund, en.bid_time.date())

        msg = "委托成功"
        if to_sell > 0:
            status = EntrustError.PARTIAL_SUCCESS
            msg += "，部分成交{}股".format(to_sell)
        else:
            status = EntrustError.SUCCESS

        result = [trade.to_json() for trade in exit_trades]
        return make_response(status, err_msg=msg, data=result)

    async def sell(
        self,
        security: str,
        bid_price: float,
        bid_shares: int,
        bid_time: datetime.datetime,
    ) -> Dict:
        """卖出委托

        Args:
            security : 委托证券代码
            price : 出售价格，如果为None，则为市价委托
            bid_shares : 询卖股数
            bid_time: 委托时间

        Returns:
            {
                "status": 0 # 0表示成功，否则为错误码
                "msg": "blah"
                "data": {

                }
            }
        """
        feed = get_app_context().feed

        logger.info("卖出委托(%s): %s %s %s", bid_time, security, bid_price, bid_shares)
        _, _, sell_limit_price = await feed.get_trade_price_limits(
            security, bid_time.date()
        )

        if bid_price is None:
            bid_type = BidType.MARKET
            bid_price = sell_limit_price
        else:
            bid_type = BidType.LIMIT

        # fill the order, get mean price
        bars = await feed.get_bars_for_match(security, bid_time)

        if self._reached_trade_price_limits(bars, bid_time, sell_limit_price):
            logger.info("撮合失败: (%s)，不能在跌停板上卖出。", security)
            return make_response(EntrustError.REACH_SELL_LIMIT)

        bars = self._remove_for_sell(bars, bid_price, sell_limit_price)

        c, v = bars["close"], bars["volume"]

        cum_v = np.cumsum(v)

        shares_to_sell = self._get_sellable_shares(security, bid_shares, bid_time)
        if shares_to_sell == 0:
            logger.info("卖出失败: %s %s %s, 可用股数为0", security, bid_shares, bid_time)
            return make_response(EntrustError.NO_POSITION)

        # until i the order can be filled
        where_total_filled = np.argwhere(cum_v >= shares_to_sell)
        if len(where_total_filled) == 0:
            i = len(v) - 1
        else:
            i = np.min(where_total_filled)

        close_time = bars[i]["frame"]
        # 也许到当天结束，都没有足够的股票
        filled = min(cum_v[i], shares_to_sell)

        # 最后一周期，只需要成交剩余的部分
        vol = v[: i + 1].copy()
        vol[-1] = filled - np.sum(vol[:-1])

        money = sum(c[: i + 1] * vol)
        mean_price = money / filled

        en = Entrust(
            security,
            EntrustSide.SELL,
            bid_shares,
            bid_price,
            bid_time,
            bid_type,
        )

        logger.info(
            "委卖%s(%s), 成交%d股，均价%.2f, 成交时间%s",
            en.security,
            en.eid,
            filled,
            mean_price,
            close_time,
        )
        return await self._fill_sell_order(en, mean_price, filled)

    def _get_sellable_shares(
        self, security: str, shares_asked: int, bid_time: datetime.datetime
    ) -> int:
        """获取可卖股数

        Args:
            security: 证券代码

        Returns:
            可卖股数
        """
        shares = 0
        for tid in self.get_unclosed_trades(bid_time.date()):
            t = self.trades[tid]
            if t.security == security and t.time.date() < bid_time.date():
                assert t.closed is False
                shares += t._unsell

        return min(shares_asked, shares)

    def _remove_for_buy(
        self, bars: np.ndarray, price: float, limit_price: float
    ) -> np.ndarray:
        """
        去掉已达到涨停时的分钟线，或者价格高于买入价的bars
        """
        close = math_round(bars["close"], 2)
        limit_price = math_round(limit_price, 2)
        return bars[(close != limit_price) & (price >= close)]

    def _remove_for_sell(
        self, bars: np.ndarray, price: float, limit_price: float
    ) -> np.ndarray:
        """去掉当前价格低于price，或者已经达到跌停时的bars,这些bars上无法成交"""
        close = math_round(bars["close"], 2)
        limit_price = math_round(limit_price, 2)
        bars = bars[(close != limit_price) & (close >= price)]

        return bars

    def _reached_trade_price_limits(
        self, bars: np.ndarray, bid_time: datetime.datetime, limit_price: float
    ) -> bool:
        cur_bar = bars[bars["frame"] == bid_time]
        if len(cur_bar) == 0:
            raise BadParameterError(f"{bid_time} not in bars for matching")

        current_price = math_round(cur_bar["close"], 2)[0]
        return current_price == math_round(limit_price, 2)
