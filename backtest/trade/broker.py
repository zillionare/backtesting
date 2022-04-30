import datetime
import logging
from typing import Dict, List, Tuple

import cfg4py
import numpy as np
from coretypes import Frame, FrameType
from empyrical import (
    annual_return,
    annual_volatility,
    calmar_ratio,
    cum_returns_final,
    max_drawdown,
    sharpe_ratio,
    sortino_ratio,
)
from omicron import array_price_equal, math_round, price_equal
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame as tf

from backtest.common.errors import AccountError, BadParameterError, NoDataForMatchError
from backtest.common.helper import get_app_context, make_response
from backtest.trade.trade import Trade
from backtest.trade.types import (
    BidType,
    Entrust,
    EntrustError,
    EntrustSide,
    position_dtype,
)

cfg = cfg4py.get_instance()
logger = logging.getLogger(__name__)


class Broker:
    # fixme: 每日资产不能简单地复制上一日的资产，因为在有持仓的情况下，只要有交易，市值都会变化。只有可用资金能复制上一日。因此，需要在更新_last_trade_day时，更新未填充日期的资产。
    def __init__(
        self,
        account_name: str,
        capital: float,
        commission: float,
        start: datetime.date = None,
        end: datetime.date = None,
    ):
        """_summary_

        Args:
            account_name : 账号/策略名
            capital : 初始本金
            commission : 佣金率
            start : 开始日期(回测时使用)
            end : 结束日期（回测时使用）
        """
        if start is not None and end is not None:
            self.mode = "bt"
            self.account_start = start
            self.account_stop = end
            self._stopped = False
        else:
            self.mode = "mock"
            self._stopped = False
            self.account_start = None
            self.account_stop = None

        self._last_trade_date = None
        self._first_trade_date = None

        self.account_name = account_name
        self.commission = commission

        # 初始本金
        self.capital = capital
        self.cash = capital  # 当前可用资金
        self._assets = {}  # 每日总资产, 包括本金和持仓资产
        self._positions = {}  # 每日持仓
        self._unclosed_trades = {}  # 未平仓的交易

        # 委托列表，包括废单和未成交委托
        self.entrusts = {}

        # 所有的成交列表，包括买入和卖出，已关闭和未关闭的
        self.trades = {}

        # trasaction = buy + sell trade
        self.transactions = []

    @property
    def account_start_date(self) -> datetime.date:
        return self.account_start or self._first_trade_date

    @property
    def account_end_date(self) -> datetime.date:
        return self.account_stop or self._last_trade_date

    @property
    def last_trade_date(self):
        return self._last_trade_date

    @property
    def first_trade_date(self):
        return self._first_trade_date

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

    def get_position(self, dt: Frame) -> List:
        """获取`dt`日持仓

        Args:
            dt : 查询哪一天的持仓

        Returns:
            返回结果为[(security, heldings, sellable, price)]，其中price为该批持仓的均价。
        """
        if len(self._positions) == 0:
            return np.array([], dtype=position_dtype)

        days = np.array(sorted(list(self._positions.keys())))
        if dt is None:
            return self._positions[days[-1]]

        if type(dt) == datetime.datetime:
            dt = dt.date()

        pos = np.argwhere(days <= dt)
        if len(pos) == 0:
            return np.array([], dtype=position_dtype)
        else:
            pos = np.argmax(pos)

        position = self._positions[days[pos]]

        # 如果获取日期大于days[pos]当前日期，则所有股份都变为可售
        if dt > days[pos]:
            position = position.copy()
            position["sellable"] = position["shares"]

        return position

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
            ‒ trades: 交易笔数
        """
        return {
            "start": self.account_start_date,
            "name": self.account_name,
            "assets": self.assets,
            "capital": self.capital,
            "last_trade": self.last_trade_date,
            "trades": len(self.trades),
            "closed": len(self.transactions),
            "earnings": self.assets - self.capital,
            "returns": self.get_returns().tolist(),
        }

    def get_returns(self, end_date: datetime.date = None) -> List[float]:
        """求截止`end_date`时的每日回报

        Args:
            end_date : _description_.

        Returns:
            _description_
        """
        dtype = [("date", "O"), ("assets", "f8")]
        assets = np.array(
            [(d, self._assets[d]) for d in sorted(self._assets.keys())], dtype=dtype
        )

        if end_date is not None:
            assets = assets[assets["date"] <= end_date]

        returns = [self.capital] + assets["assets"]

        return np.diff(returns) / returns[:-1]

    @property
    def assets(self) -> float:
        """当前总资产。

        如果要获取历史上某天的总资产，请使用`get_assets`方法。
        """
        if self.last_trade_date is None:
            return self.capital

        return self.get_assets(None)

    def get_assets(self, date: datetime.date) -> float:
        """计算某日的总资产

        assets在每次交易后都会更新，所以可以通过计算assets来计算某日的总资产。如果某日不存在交易，则返回上一笔交易后的资产。

        Args:
            date : 查询哪一天的资产

        Returns:
            返回总资产

        """
        if len(self._assets) == 0:
            return self.capital

        days = np.array(sorted(list(self._assets.keys())))
        if date is None:
            return self._assets[days[-1]]

        if type(date) == datetime.datetime:
            date = date.date()

        pos = np.argwhere(days <= date)
        if len(pos) == 0:  # 时间在所有的交易日之前，因此返回本金
            return self.capital

        pos = np.argmax(pos)
        return self._assets[days[pos]]

    @property
    def position(self):
        """获取当前持仓

        如果要获取历史上某天的持仓，请使用`get_position`方法。

        Returns:
            返回成员为(code, shares, sellable, price)的numpy structure array
        """
        if self.last_trade_date is None:
            return np.array([], dtype=position_dtype)

        return self.get_position(None)

    def __str__(self):
        s = (
            f"账户：{self.account_name}:\n"
            + f"    总资产：{self.assets:,.2f}\n"
            + f"    本金：{self.capital:,.2f}\n"
            + f"    可用资金：{self.cash:,.2f}\n"
            + f"    持仓：{self.position}\n"
        )

        return s

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>{self}"

    def _update_trade_date(self, bid_time: Frame):
        """根据bid_time，

        Args:
            bid_time : _description_
        """
        if type(bid_time) == datetime.datetime:
            bid_time = bid_time.date()

        if self._first_trade_date is None:
            self._first_trade_date = bid_time
        elif bid_time < self._first_trade_date:
            logger.warning("委托日期必须递增出现: %s -> %s", self._first_trade_date, bid_time)
            raise ValueError(f"委托日期必须递增出现, {bid_time} -> {self._first_trade_date}")

        if self._last_trade_date is None or bid_time >= self._last_trade_date:
            self._last_trade_date = bid_time
        else:
            logger.warning("委托日期必须递增出现：%s -> %s", self._last_trade_date, bid_time)
            raise ValueError(
                f"委托日期必须递增出现, {self._last_trade_date} -> {self._last_trade_date}"
            )

        if self.mode == "bt" and bid_time > self.account_stop:
            self._stopped = True
            logger.warning("委托时间超过回测结束时间: %s, %s", bid_time, self.account_stop)
            raise AccountError(f"委托时间超过回测结束时间，{self.account_stop} -> {bid_time}")

    async def buy(
        self,
        security: str,
        bid_price: float,
        bid_shares: int,
        bid_time: datetime.datetime,
    ) -> Dict:
        """买入委托

        买入以尽可能实现委托为目标。如果可用资金不足，但能买入部分股票，则部分买入。

        如果bid_price为None，则使用涨停价买入。

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
        self._update_trade_date(bid_time)

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
            "买入委托(%s): %s %d %s, 单号：%s",
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
        if bars.size == 0:
            logger.warning("failed to match %s, no data at %s", security, bid_time)
            raise NoDataForMatchError(f"没有{security}在{bid_time}当天的数据")

        # 移除掉涨停和价格高于委买价的bar后，看还能买多少股
        bars, status = self._remove_for_buy(bars, bid_price, buy_limit_price)
        if bars is None:
            logger.info("委买失败：%s, %s, reason: %s", security, bid_time, status)
            return {"status": status, "msg": str(status), "data": None}

        # 将买入数限制在可用资金范围内
        shares_to_buy = min(
            bid_shares, self.cash // (bid_price * (1 + self.commission))
        )

        # 必须以手为单位买入，否则委托会失败
        shares_to_buy = shares_to_buy // 100 * 100
        if shares_to_buy < 100:
            logger.info("委买失败：%s(%s), 资金(%s)不足", security, self.cash, en.eid)
            return make_response(EntrustError.NO_CASH)

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

    def _update_unclosed_trades(self, tid, date: datetime.date):
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
        self._update_unclosed_trades(trade.tid, close_time.date())
        self._update_position(trade, close_time.date())

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

        return {
            "status": status,
            "msg": msg,
            "data": trade,
        }

    def _update_position(self, trade: Trade, bid_date: datetime.date):
        """更新持仓信息

        持仓信息按日组织为dict, 其value为numpy一维数组，包含当日持仓的所有股票的代码、持仓量、可售数量和均价。

        Args:
            trade: 交易信息
            bid_date: 买入/卖出日期
        """
        if type(bid_date) == datetime.datetime:
            bid_date = bid_date.date()

        # find and copy
        position = self.get_position(bid_date)

        if position is None:
            self._positions[bid_date] = np.array(
                [(trade.security, trade.shares, 0, trade.price)], dtype=position_dtype
            )
        else:
            position = position.copy()
            if np.any(position["security"] == trade.security):
                # found and merge
                i = np.where(position["security"] == trade.security)[0][0]

                _, old_shares, old_sellable, old_price = position[i]
                new_shares, new_price = trade.shares, trade.price

                if trade.side == EntrustSide.BUY:
                    position[i] = (
                        trade.security,
                        old_shares + trade.shares,
                        old_sellable,
                        (old_price * old_shares + new_shares * new_price)
                        / (old_shares + new_shares),
                    )
                else:
                    shares = old_shares - trade.shares
                    sellable = old_sellable - trade.shares
                    if shares == 0:
                        position = np.delete(position, i, axis=0)
                    else:
                        position[i] = (
                            trade.security,
                            shares,
                            sellable,
                            old_price,  # 卖出时成本不变
                        )
            else:
                position = np.concatenate(
                    (
                        position,
                        np.array(
                            [(trade.security, trade.shares, 0, trade.price)],
                            dtype=position_dtype,
                        ),
                    )
                )

            self._positions[bid_date] = position

    async def _update_assets(self, cash_change: float, dt: datetime.date):
        """更新当前资产（含持仓）

        在每次资产变动时进行计算和更新。

        Args:
            cash_change : 变动的现金
            dt: 当前资产（持仓）所属于的日期
        """
        if type(dt) == datetime.datetime:
            dt = dt.date()

        self.cash += cash_change

        positions = self.get_position(dt)

        held_secs = [pos[0] for pos in positions]

        market_value = 0

        # 如果使用空列表去查询数据，会耗时较长
        if len(held_secs) > 0:
            feed = get_app_context().feed
            closes = await feed.get_close_price(held_secs, dt)

            for sec, shares, sellable, _ in positions:
                market_value += math_round(closes[sec], 2) * shares

        self._assets[dt] = self.cash + market_value
        logger.info(
            f"资产更新({dt})<总资产:{self.assets:,.2f} 可用:{self.cash:,.2f} 本金:{self.capital:,.2f}>"
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
                self._update_position(exit_trade, exit_trade.time)
                exit_trades.append(exit_trade)
                self.trades[exit_trade.tid] = exit_trade
                self.transactions.append(tx)

                refund += exit_trade.shares * exit_trade.price - exit_trade.fee

                if trade.closed:
                    closed_trades.append(tid)

                if to_sell == 0:
                    break
            else:  # no more unclosed trades, even if to_sell > 0
                break

        unclosed_trades = [tid for tid in unclosed_trades if tid not in closed_trades]
        self._unclosed_trades[dt] = unclosed_trades

        await self._update_assets(refund, en.bid_time.date())

        msg = "委托成功"
        if to_sell > 0:
            status = EntrustError.PARTIAL_SUCCESS
            msg += "，部分成交{}股".format(to_sell)
        else:
            status = EntrustError.SUCCESS

        return {
            "status": status,
            "msg": msg,
            "data": exit_trades,
        }

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
        self._update_trade_date(bid_time)

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
        if bars.size == 0:
            logger.warning("failed to match: %s, no data at %s", security, bid_time)
            raise NoDataForMatchError(f"No data for {security} at {bid_time}")

        bars, status = self._remove_for_sell(bars, bid_price, sell_limit_price)
        if bars is None:
            logger.info("委卖失败：%s, %s, reason: %s", security, bid_time, status)
            return {"status": status, "msg": str(status), "data": None}

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
        reach_limit = array_price_equal(bars["close"], limit_price)
        bars = bars[(~reach_limit)]

        if bars.size == 0:
            return None, EntrustError.REACH_BUY_LIMIT

        bars = bars[(bars["close"] <= price)]
        if bars.size == 0:
            return None, EntrustError.PRICE_NOT_MEET

        return bars, None

    def _remove_for_sell(
        self, bars: np.ndarray, price: float, limit_price: float
    ) -> np.ndarray:
        """去掉当前价格低于price，或者已经达到跌停时的bars,这些bars上无法成交"""
        reach_limit = array_price_equal(bars["close"], limit_price)
        bars = bars[(~reach_limit)]

        if bars.size == 0:
            return None, EntrustError.REACH_SELL_LIMIT

        bars = bars[(bars["close"] >= price)]
        if bars.size == 0:
            return None, EntrustError.PRICE_NOT_MEET

        return bars, None

    def _reached_trade_price_limits(
        self, bars: np.ndarray, bid_time: datetime.datetime, limit_price: float
    ) -> bool:
        cur_bar = bars[bars["frame"] == bid_time]
        if len(cur_bar) == 0:
            raise BadParameterError(f"{bid_time} not in bars for matching")

        return price_equal(cur_bar["close"], limit_price)

    def freeze(self):
        """冻结账户，停止接收新的委托"""
        self._stopped = True

    async def metrics(
        self,
        start: datetime.date = None,
        end: datetime.date = None,
        baseline: str = None,
    ) -> Dict:
        """
        获取指定时间段的账户指标

        Args:
            start: 开始时间
            end: 结束时间
            baseline: 参考标的

        Returns:
            - start 回测起始时间
            - end   回测结束时间
            - window 资产暴露时间
            - total_tx 发生的配对交易次数
            - total_profit 总盈亏
            - total_profit_rate 总盈亏率
            - win_rate 胜率
            - mean_return 每笔配对交易平均回报率
            - sharpe    夏普比率
            - max_drawdown 最大回撤
            - sortino
            - calmar
            - annual_return 年化收益率
            - volatility 波动率
            - baseline: dict
                - win_rate
                - sharpe
                - max_drawdown
                - sortino
                - annual_return
                - total_profit_rate
                - volatility
        """
        try:
            rf = cfg.metrics.risk_free_rate / cfg.metrics.annual_days
        except Exception:
            rf = 0

        start = min(start or self.account_start_date, self.account_start_date)
        end = max(end or self.last_trade_date, self.account_start_date)

        tx = []
        for t in self.transactions:
            if t.entry_time.date() >= start and t.exit_time.date() <= end:
                tx.append(t)

        # 资产暴露时间
        window = tf.count_day_frames(start, end)
        total_tx = len(tx)

        if total_tx == 0:
            return {
                "start": start,
                "end": end,
                "window": window,
                "total_tx": total_tx,
                "total_profit": None,
                "total_profit_rate": None,
                "win_rate": None,
                "mean_return": None,
                "sharpe": None,
                "sortino": None,
                "calmar": None,
                "max_drawdown": None,
                "annual_return": None,
                "volatility": None,
                "baseline": None,
            }

        # win_rate
        wr = len([t for t in tx if t.profit > 0]) / total_tx

        assets = []
        for dt in tf.get_frames(start, end, FrameType.DAY):
            date = tf.int2date(dt)
            assets.append(self.get_assets(date))

        total_profit = assets[-1] - self.capital

        assets = np.array(assets)
        returns = assets[1:] / assets[:-1] - 1
        mean_return = np.mean(returns)

        sharpe = sharpe_ratio(returns, rf)
        sortino = sortino_ratio(returns, rf)
        calma = calmar_ratio(returns)
        mdd = max_drawdown(returns)

        # 年化收益率
        ar = annual_return(returns)

        # 年化波动率
        vr = annual_volatility(returns)

        # 计算参考标的的相关指标
        if baseline is not None:
            ref_bars = await Stock.get_bars_in_range(
                baseline, FrameType.DAY, start, end
            )

            if ref_bars.size < 2:
                ref_results = None
            else:
                returns = ref_bars["close"][1:] / ref_bars["close"][:-1] - 1

                ref_results = {
                    "code": baseline,
                    "win_rate": np.count_nonzero(returns > 0) / len(returns),
                    "sharpe": sharpe_ratio(returns, rf),
                    "max_drawdown": max_drawdown(returns),
                    "sortino": sortino_ratio(returns, rf),
                    "annual_return": annual_return(returns),
                    "total_profit_rate": cum_returns_final(returns),
                    "volatility": annual_volatility(returns),
                }
        else:
            ref_results = None

        return {
            "start": start,
            "end": end,
            "window": window,
            "total_tx": total_tx,
            "total_profit": total_profit,
            "total_profit_rate": total_profit / self.capital,
            "win_rate": wr,
            "mean_return": mean_return,
            "sharpe": sharpe,
            "sortino": sortino,
            "calmar": calma,
            "max_drawdown": mdd,
            "annual_return": ar,
            "volatility": vr,
            "baseline": ref_results,
        }


if __name__ == "__main__":
    import asyncio

    import omicron
    from sanic import Sanic

    app = Sanic("backtest")

    broker = Broker("aaron", 1_000_000, 1.5e-4)
    bid_time = datetime.datetime(2022, 3, 18, 9, 35)

    async def init_and_buy(sec, price, shares, bid_time):
        import os

        from backtest.config import get_config_dir
        from backtest.feed.basefeed import BaseFeed

        feed = await BaseFeed.create_instance(interface="zillionare")
        app.ctx.feed = feed

        cfg4py.init("~/zillionare/backtest/config")
        await omicron.init()

        broker = Broker("aaron", 1_000_000, 1.5e-4)
        await broker.buy(sec, price, shares, bid_time)

    asyncio.run(init_and_buy("000001.XSHE", 14.7, 100, bid_time))
