import asyncio
import datetime
import logging
from typing import Dict, List, Tuple

import arrow
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
from pyemit import emit

from backtest.common.errors import AccountError, BadParameterError, NoDataForMatchError
from backtest.common.helper import get_app_context, make_response, tabulate_numpy_array
from backtest.trade.trade import Trade
from backtest.trade.types import (
    E_BACKTEST,
    BidType,
    Entrust,
    EntrustError,
    EntrustSide,
    assets_dtype,
    cash_dtype,
    daily_position_dtype,
    float_ts_dtype,
    position_dtype,
)

cfg = cfg4py.get_instance()
logger = logging.getLogger(__name__)


class Broker:
    def __init__(
        self,
        account_name: str,
        capital: float,
        commission: float,
        bt_start: datetime.date = None,
        bt_end: datetime.date = None,
    ):
        """_summary_

        Args:
            account_name : 账号/策略名
            capital : 初始本金
            commission : 佣金率
            start : 开始日期(回测时使用)
            end : 结束日期（回测时使用）
        """
        if bt_start is not None and bt_end is not None:
            self.mode = "bt"
            self.bt_start = bt_start
            self.bt_stop = bt_end
            # 回测是否终止？
            self._bt_stopped = False
        else:
            self.mode = "mock"
            self._bt_stopped = False
            self.bt_start = None
            self.bt_stop = None

        # 最后交易时间
        self._last_trade_date = None
        self._first_trade_date = None

        self.account_name = account_name
        self.commission = commission

        # 初始本金
        self.capital = capital
        # 每日盘后可用资金
        self._cash = np.array([], dtype=cash_dtype)
        # 每日总资产, 包括本金和持仓资产
        self._assets = np.array([], dtype=assets_dtype)

        self._positions = np.array([], dtype=daily_position_dtype)  # 每日持仓
        self._unclosed_trades = {}  # 未平仓的交易

        # 委托列表，包括废单和未成交委托
        self.entrusts = {}

        # 所有的成交列表，包括买入和卖出，已关闭和未关闭的
        self.trades = {}

        # trasaction = buy + sell trade
        self.transactions = []

        self._lock = asyncio.Lock()

    def __getstate__(self):
        """self._lock is not pickable"""
        state = self.__dict__.copy()
        del state["_lock"]

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = asyncio.Lock()

    @property
    def lock(self):
        return self._lock

    @property
    def cash(self):
        if self._cash.size == 0:
            return self.capital

        return self._cash[-1]["cash"].item()

    @property
    def account_start_date(self) -> datetime.date:
        return self.bt_start or self._first_trade_date

    @property
    def account_end_date(self) -> datetime.date:
        return self.bt_stop or self._last_trade_date

    @property
    def last_trade_date(self):
        return self._last_trade_date

    @property
    def first_trade_date(self):
        return self._first_trade_date

    def get_cash(self, dt: datetime.date) -> float:
        """获取`dt`当天的可用资金

        在查询时，如果`dt`小于首次交易日，则返回空，否则，如果当日无数据，将从上一个有数据之日起，进行补齐填充。
        Args:
            dt (datetime.date): 日期

        Returns:
            float: 某日可用资金
        """
        if self._cash.size == 0:
            return self.capital

        if dt > self._cash[-1]["date"]:
            return self._cash[-1]["cash"].item()
        elif dt < self._cash[0]["date"]:
            return self.capital

        result = self._cash[self._cash["date"] == dt]["cash"]
        if result.size == 0:
            raise ValueError(f"{dt} not found")
        else:
            return result.item()

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

    def get_position(self, dt: datetime.date, dtype=position_dtype) -> List:
        """获取`dt`日持仓

        如果传入的`dt`大于持仓数据的最后一天，将返回最后一天的持仓数据,并且所有持仓均为可售状态
        如果传入的`dt`小于持仓数据的第一天，将返回空。

        Args:
            dt : 查询哪一天的持仓
            dtype : 返回数据类型，可为position_dtype或daily_position_dtype，后者用于日志输出

        Returns:
            返回结果为dtype为`dtype`的一维numpy structured array，其中price为该批持仓的均价。
        """
        if self._positions.size == 0:
            return np.array([], dtype=dtype)

        if dt < self._positions[0]["date"]:
            return np.array([], dtype=dtype)

        last_date = self._positions[-1]["date"]
        if dt > last_date:
            result = self._positions[self._positions["date"] == last_date]
            result["sellable"] = result["shares"]
            return result[list(dtype.names)].astype(dtype)

        result = self._positions[self._positions["date"] == dt]
        if result.size == 0:
            raise ValueError(f"{dt} not found")

        return result[list(dtype.names)].astype(dtype)

    async def recalc_assets(self):
        """重新计算资产"""
        if self.mode == "bt":
            end = self.bt_stop
            start = self.bt_start
        else:
            end = arrow.now().date()
            start = self._first_trade_date
            if start is None:
                return

        # 把期初资产加进来
        _before_start = tf.day_shift(start, -1)
        self._assets = np.array([(_before_start, self.capital)], dtype=assets_dtype)
        frames = tf.get_frames(start, end, FrameType.DAY)
        for frame in frames:
            date = tf.int2date(frame)
            await self._calc_assets(date)

    async def info(self) -> Dict:
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
        await self.recalc_assets()
        returns = await self.get_returns(recalc_assets=False)
        return {
            "start": self.account_start_date,
            "name": self.account_name,
            "assets": self.assets,
            "capital": self.capital,
            "last_trade": self.last_trade_date,
            "trades": len(self.trades),
            "closed": len(self.transactions),
            "earnings": self.assets - self.capital,
            "returns": returns,
        }

    async def get_returns(
        self,
        start_date: datetime.date = None,
        end_date: datetime.date = None,
        recalc_assets: bool = True,
    ) -> np.ndarray:
        """求截止`end_date`时的每日回报

        Args:
            start_date: 计算回报的起始日期
            end_date : 计算回报的结束日期

        Returns:
            以百分比为单位的每日回报率,索引为对应日期
        """
        start = start_date or self.account_start_date

        # 当计算[start, end]之间的每日回报时，需要取多一日，即`start`之前一日的总资产
        _start = tf.day_shift(start, -1)
        end = end_date or self.account_end_date

        assert self.account_start_date <= start <= end
        assert start <= end <= self.account_end_date

        if recalc_assets:
            await self.recalc_assets()

        assets = self._assets[
            (self._assets["date"] >= _start) & (self._assets["date"] <= end)
        ]

        if assets.size == 0:
            raise ValueError(f"date range error: {start} - {end} contains no data")

        assets = assets.astype(float_ts_dtype)
        assets["value"][1:] = assets["value"][1:] / assets["value"][:-1] - 1
        assets["value"][0] = 0

        return assets

    @property
    def assets(self) -> float:
        """当前总资产。

        如果要获取历史上某天的总资产，请使用`get_assets`方法。
        """
        if self._assets.size == 0:
            return self.capital
        else:
            return self._assets[-1]["assets"]

    async def get_assets(self, date: datetime.date) -> float:
        """查询某日的总资产

        当日总资产 = 当日可用资金 + 持仓市值

        Args:
            date : 查询哪一天的资产

        Returns:
            返回总资产

        """
        if self._assets.size == 0:
            return self.capital

        result = self._assets[self._assets["date"] == date]
        if result.size == 1:
            return result["assets"].item()

        assets, *_ = await self._calc_assets(date)
        return assets

    async def _calc_assets(self, date: datetime.date) -> Tuple[float]:
        """计算某日的总资产，并缓存

        Args:
            date : 计算哪一天的资产

        Returns:
            返回总资产, 可用资金, 持仓市值
        """
        if date < self.account_start_date:
            return self.capital, 0, 0

        if (self.mode == "bt" and date > self.bt_stop) or date > arrow.now().date():
            raise ValueError(
                f"wrong date: {date}, date must be before {self.bt_stop} or {arrow.now().date()}"
            )

        cash = self.get_cash(date)
        positions = self.get_position(date)
        heldings = positions[positions["shares"] > 0]["security"]

        market_value = 0
        if heldings.size > 0:
            feed = get_app_context().feed

            prices = await feed.get_close_price(heldings, date)
            for code, price in prices.items():
                shares = positions[positions["security"] == code]["shares"].item()
                market_value += shares * price

        assets = cash + market_value

        if date not in self._assets:
            self._assets = np.append(
                self._assets, np.array([(date, assets)], dtype=assets_dtype)
            )
        else:
            self._assets[self._assets["date"] == date]["assets"] = assets

        return assets, cash, market_value

    @property
    def position(self):
        """获取当前持仓

        如果要获取历史上某天的持仓，请使用`get_position`方法。

        Returns:
            返回成员为(code, shares, sellable, price)的numpy structure array
        """
        if self._positions.size == 0:
            return np.array([], dtype=position_dtype)

        last_day = self._positions[-1]["date"]
        return self._positions[self._positions["date"] == last_day]

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

    def _calendar_validation(self, bid_time: datetime.date):
        """更新和校准交易日期

        Args:
            bid_time : 交易发生的时间
        """
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

        if self.mode == "bt" and bid_time > self.bt_stop:
            self._bt_stopped = True
            logger.warning("委托时间超过回测结束时间: %s, %s", bid_time, self.bt_stop)
            raise AccountError(f"委托时间超过回测结束时间，{self.bt_stop} -> {bid_time}")

    async def buy(self, *args, **kwargs):
        """同一个账户，也可能出现并发的买单和卖单，这些操作必须串行化"""
        async with self.lock:
            return await self._buy(*args, **kwargs)

    async def _buy(
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
        await self._before_trade(bid_time)

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

        logger.info(
            "%s 买入后持仓: \n%s",
            close_time.date(),
            tabulate_numpy_array(
                self.get_position(close_time.date(), daily_position_dtype)
            ),
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

        await emit.emit(E_BACKTEST, {"buy": trade})
        return {"status": status, "msg": msg, "data": trade}

    async def _before_trade(self, bid_time: Frame):
        """交易前的准备工作

        在每次交易前，补齐每日现金数据和持仓数据到`date`，更新账户生命期等。

        Args:
            date: 日期

        Returns:
            无
        """
        if type(bid_time) == datetime.datetime:
            bid_time = bid_time.date()

        self._calendar_validation(bid_time)

        # 补齐可用将资金表
        if self._cash.size > 0:
            assert bid_time >= self._cash[0]["date"]
            last_dt, cash = self._cash[-1]

            frames = tf.get_frames(last_dt, bid_time, FrameType.DAY)[1:]
            if frames.size > 0:
                recs = [(tf.int2date(date), cash) for date in frames]

                self._cash = np.concatenate(
                    (self._cash, np.array(recs, dtype=cash_dtype))
                )

        # 补齐持仓表
        if self._positions.size > 0:
            assert bid_time >= self._positions[0]["date"]
            assert last_dt == self._positions[-1]["date"], "可用资金表与持仓表不同步"

            last_dt = self._positions[-1]["date"]

            frames = tf.get_frames(last_dt, bid_time, FrameType.DAY)[1:]
            if frames.size > 0:
                data = self._positions.tolist()

                last_day_position = self._positions[self._positions["date"] == last_dt]
                for frame in frames:
                    copy = last_day_position.copy()

                    copy["date"] = tf.int2date(frame)
                    # 延后一日后，持仓全部可用
                    copy["sellable"] = copy["shares"]
                    data.extend(copy)

                self._positions = np.array(data, dtype=daily_position_dtype)

    def _update_position(self, trade: Trade, bid_date: datetime.date):
        """更新持仓信息

        持仓信息为一维numpy数组，其类型为daily_position_dtype。如果某支股票在某日被清空，则当日持仓表保留记录，置shares为零，方便通过持仓表看出股票的进场出场时间，另一方面，如果不保留这条记录（而是删除），则在所有股票都被清空的情况下，会导致持仓表出现空洞

        Args:
            trade: 交易信息
            bid_date: 买入/卖出日期
        """
        if type(bid_date) == datetime.datetime:
            bid_date = bid_date.date()

        if self._positions.size == 0:
            self._positions = np.array(
                [(bid_date, trade.security, trade.shares, 0, trade.price)],
                dtype=daily_position_dtype,
            )

            return

        # find if the security is already in the position (same day)
        pos = np.argwhere(
            (self._positions["security"] == trade.security)
            & (self._positions["date"] == bid_date)
        )

        if pos.size == 0:
            self._positions = np.append(
                self._positions,
                np.array(
                    [(bid_date, trade.security, trade.shares, 0, trade.price)],
                    dtype=daily_position_dtype,
                ),
            )
        else:
            i = pos[0].item()
            *_, old_shares, old_sellable, old_price = self._positions[i]
            new_shares, new_price = trade.shares, trade.price

            if trade.side == EntrustSide.BUY:
                self._positions[i] = (
                    bid_date,
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
                    old_price = 0
                self._positions[i] = (
                    bid_date,
                    trade.security,
                    shares,
                    sellable,
                    old_price,  # 卖出时成本不变，除非已清空
                )

        return

    async def _update_assets(self, cash_change: float, dt: datetime.date):
        """更新当前资产（含持仓）

        在每次资产变动时进行计算和更新。

        Args:
            cash_change : 变动的现金
            dt: 当前资产（持仓）所属于的日期
        """
        logger.info("cash change: %s", cash_change)
        if type(dt) == datetime.datetime:
            dt = dt.date()

        if self._cash.size == 0:
            self._cash = np.array([(dt, self.capital + cash_change)], dtype=cash_dtype)
        else:
            # _before_trade应该已经为当日交易准备好了可用资金数据
            assert self._cash[-1]["date"] == dt
            self._cash[-1]["cash"] += cash_change

        assets, cash, mv = await self._calc_assets(dt)

        info = np.array(
            [(dt, assets, cash, mv, cash_change)],
            dtype=[
                ("date", "O"),
                ("assets", float),
                ("cash", float),
                ("market value", float),
                ("change", float),
            ],
        )
        logger.info("\n%s", tabulate_numpy_array(info))

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
                trade: Trade = self.trades[tid]
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

        logger.info(
            "%s 卖出后持仓: \n%s",
            dt,
            tabulate_numpy_array(self.get_position(dt, daily_position_dtype)),
        )

        await self._update_assets(refund, en.bid_time.date())

        msg = "委托成功"
        if to_sell > 0:
            status = EntrustError.PARTIAL_SUCCESS
            msg += "，部分成交{}股".format(to_sell)
        else:
            status = EntrustError.SUCCESS

        await emit.emit(E_BACKTEST, {"sell": exit_trades})
        return {"status": status, "msg": msg, "data": exit_trades}

    async def sell(self, *args, **kwargs):
        """同一个账户，也可能出现并发的买单和卖单，这些操作必须串行化"""
        async with self.lock:
            return await self._sell(*args, **kwargs)

    async def _sell(
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
        await self._before_trade(bid_time)

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
            security, EntrustSide.SELL, bid_shares, bid_price, bid_time, bid_type
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
        self._bt_stopped = True

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
        end = max(end or self.account_end_date, self.account_end_date)

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

        await self.recalc_assets()

        # 当计算[start, end]之间的盈亏时，我们实际上要多取一个交易日，即start之前一个交易日的资产数据
        _start = tf.day_shift(start, -1)
        total_profit = await self.get_assets(end) - await self.get_assets(_start)

        returns = (await self.get_returns(start, end, False))["value"]
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
