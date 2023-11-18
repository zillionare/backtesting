"""
Broker是一个交易代理。每一个交易代理对应一个账户，记录了该账户下的交易记录、每日持仓记录和每日市值记录等数据，并提供交易撮合的具体实现。

在每次交易(buy, sell)之前，都将执行_before_trade，以将现金表、持仓表和资产表更新到`bid_time`前一交易日。支持以下查询：
* get_position
* get_cash
* get_assets
"""
import asyncio
import datetime
import logging
import uuid
from typing import Dict, List, Optional, Tuple, Union

import arrow
import cfg4py
import numpy as np
import pandas as pd
from coretypes import Frame, FrameType
from coretypes.errors.trade import (
    AccountStoppedError,
    BadParamsError,
    BuylimitError,
    CashError,
    NoDataForMatch,
    PositionError,
    PriceNotMeet,
    SellLimitError,
    TimeRewindError,
    TradeError,
    VolumeNotMeet,
)
from deprecation import deprecated
from empyrical import (
    annual_return,
    annual_volatility,
    calmar_ratio,
    cum_returns_final,
    max_drawdown,
    sharpe_ratio,
    sortino_ratio,
)
from numpy.typing import NDArray
from omicron.core.backtestlog import BacktestLogger
from omicron.extensions import array_math_round, array_price_equal, math_round
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame as tf
from pyemit import emit

from backtest.common.helper import get_app_context, jsonify, tabulate_numpy_array
from backtest.trade.datatypes import (
    E_BACKTEST,
    BidType,
    Entrust,
    EntrustSide,
    assets_dtype,
    cash_dtype,
    daily_position_dtype,
    float_ts_dtype,
    position_dtype,
    rich_assets_dtype,
)
from backtest.trade.trade import Trade
from backtest.trade.transaction import Transaction

cfg = cfg4py.get_instance()
logger = BacktestLogger.getLogger(__name__)
entrustlog = logging.getLogger("entrust")
tradelog = logging.getLogger("trade")


class Broker:
    def __init__(
        self,
        account_name: str,
        principal: float,
        commission: float,
        bt_start: datetime.date,
        bt_end: datetime.date,
    ):
        """创建一个Broker对象

        Args:
            account_name : 账号/策略名
            principal : 初始本金
            commission : 佣金率
            start : 开始日期(回测时使用)
            end : 结束日期（回测时使用）
        """
        self.bt_start = bt_start
        self.bt_end = bt_end
        # 回测是否终止？
        self._bt_stopped = False
        start = tf.day_shift(bt_start, -1)

        # 每日盘后可用资金
        self._cash = np.array([(start, principal)], dtype=cash_dtype)

        # 每日持仓
        self._positions = np.array(
            [(start, None, 0, 0, 0)],
            dtype=daily_position_dtype,
        )

        # 每日总资产(盘后), 包括本金和持仓资产
        self._assets = np.array([(start, principal)], dtype=assets_dtype)

        # 最后交易时间
        self._last_trade_time: Optional[datetime.datetime] = None
        self._first_trade_time: Optional[datetime.datetime] = None

        self.account_name = account_name
        self.commission = commission

        # 初始本金
        self.principal = principal
        self._unclosed_trades: Dict[datetime.date, List[str]] = {}  # 未平仓的交易

        # 委托列表，包括废单和未成交委托
        self.entrusts = {}

        # 所有的成交列表，包括买入和卖出，已关闭和未关闭的
        self.trades = {}

        # trasaction = buy + sell trade
        self.transactions: List[Transaction] = []

        self._lock = asyncio.Lock()

    @deprecated("since 0.5.0, pickle bills and metrics instead")
    def __getstate__(self):
        # self._lock is not pickable
        state = self.__dict__.copy()
        del state["_lock"]

        return state

    @deprecated("since 0.5.0, pickle bills and metrics instead")
    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = asyncio.Lock()

    @property
    def lock(self):
        return self._lock

    @property
    def cash(self):
        return self._cash[-1]["cash"].item()

    @property
    def last_trade_date(self):
        return None if self._last_trade_time is None else self._last_trade_time.date()

    @property
    def first_trade_date(self):
        return None if self._first_trade_time is None else self._first_trade_time.date()

    def get_cash(self, dt: datetime.date) -> float:
        """获取`dt`当天的可用资金

        如果`dt`早于起始日将抛出异常。否则，返回小于等于`dt`的最后一个交易日的可用资金。

        Args:
            dt (datetime.date): 日期

        Returns:
            float: 某日可用资金
        """
        if dt < self.bt_start:
            raise BadParamsError(f"dt should be later than start {self.bt_start}")

        ipos = np.argwhere(dt >= self._cash["date"]).flatten()
        if len(ipos) == 0:
            return self.principal

        ipos = ipos[-1]
        return self._cash[ipos]["cash"].item()

    def get_unclosed_trades(self, dt: datetime.date) -> List[str]:
        """获取`dt`当天未平仓的交易

        如果`dt`小于首次交易日，则返回空，否则，如果当日无数据，将从上一个有数据之日起，进行补齐填充。
        """
        if len(self._unclosed_trades) == 0:
            return []

        result = self._unclosed_trades.get(dt)
        if result is None:
            start = sorted(self._unclosed_trades.keys())[0]
            if dt < start:
                return []
            else:
                self._forward_unclosed_trades(dt)

        return self._unclosed_trades.get(dt, [])

    def get_position(self, dt: datetime.date, dtype=position_dtype) -> np.ndarray:
        """获取`dt`日持仓

        如果传入的`dt`大于持仓数据的最后一天，将返回最后一天的持仓数据,并且所有持仓均为可售状态
        如果传入的`dt`小于持仓数据的第一天，将返回空。

        Args:
            dt : 查询哪一天的持仓
            dtype : 返回的数据类型，可为[position_dtype][backtest.trade.datatypes.position_dtype]或[daily_position_dtype][backtest.trade.datatypes.daily_position_dtype]，后者用于日志输出

        Returns:
            返回结果为dtype为`dtype`的一维numpy structured array，其中price为该批持仓的均价。
        """
        if dt < self._positions[0]["date"]:
            return np.array([], dtype=dtype)

        last_date = np.max(self._positions["date"])
        if dt > last_date:
            filter = (self._positions["date"] == last_date) & (
                self._positions["security"] != None  # noqa: E711
            )
            result = self._positions[filter]
            result["sellable"] = result["shares"]
            return result[list(dtype.names)].astype(dtype)  # type: ignore

        filter = (self._positions["date"] == dt) & (
            self._positions["security"] != None  # noqa: E711
        )
        result = self._positions[filter]

        return result[list(dtype.names)].astype(dtype)  # type: ignore

    async def _query_market_values(
        self, start: datetime.date, end: datetime.date
    ) -> pd.Series:
        frames = [tf.int2date(d) for d in tf.get_frames(start, end, FrameType.DAY)]

        filter = (self._positions["date"] >= start) & (self._positions["date"] <= end)
        secs = list(set(self._positions[filter]["security"]))

        if len(secs):
            feed = get_app_context().feed
            df_prices = await feed.batch_get_close_price_in_range(secs, start, end)
        else:
            df_prices = pd.DataFrame([], index=frames)

        # 1. get shares of each day in range [start, end]
        df_shares = pd.DataFrame(data=self._positions[filter]).pivot(
            columns="security", index="date", values="shares"
        )

        # 2. df_shares * df_close then sum on columns => mv
        mv = pd.DataFrame([], index=frames)
        mv = df_prices.multiply(df_shares).sum(axis=1)
        return mv

    async def _forward_assets(self, end: datetime.date):
        """更新资产表

        计算时，将从当前资产表最后一条记录（含）起，计算到`end`与cash表、position表中的的最后记录为止。

        Note: bt_stop调用时，需要补齐cash,position与asset表

        !!! Note:
            如果再两次调用本方法中间，存在交易，则最后一条记录可能发生变化。

        本函数只会补齐 assets， 而不会补齐cash和position，这两者应该在before_trade中补齐。
        Args:
            end: 计算到哪一天的资产。
        """
        start = self._assets[-1]["date"]

        cash_end = self._cash[-1]["date"]
        pos_end = self._positions[-1]["date"]

        if pos_end != cash_end:
            msg = f"cash table {cash_end} is not synced with position table {pos_end}"
            raise TradeError(msg, with_stack=True)

        end = min(end, cash_end)
        if start > end:
            return

        # 待补齐的资产日
        mv = await self._query_market_values(start, end)
        # cash + mv
        filter = (self._cash["date"] >= start) & (self._cash["date"] <= end)
        assets = mv + self._cash[filter]["cash"]

        self._assets = np.append(
            self._assets[:-1],  # 最后一行是重叠的
            assets.to_frame().to_records(index=True).astype(self._assets.dtype),
        )

    async def info(self, dt: Optional[datetime.date] = None) -> Dict:
        """`dt`日的账号相关信息

        Returns:
            Dict: 账号相关信息：np.array([
            (mar1, tyst, 500, 0, 0),
            (mar2, tyst, 1000, 500, 0),
            (mar2, hljh, 500, 0, 0),
            (mar3, hljh, 500, 500, 0),
            (mar3, tyst, 1000, 1000, 0),
            (mar4, hljh, 500, 500, 0),
            (mar4, tyst, 1000, 1000, 0),
            (mar7, tyst, 500, 500, 0, 0),
            (mar8, None, 0, 0, 0)
        ], dtype=daily_position_dtype)

            - name: str, 账户名
            - principal: float, 初始资金
            - assets: float, `dt`日资产
            - start: datetime.date, 账户创建时间
            - end: 账户结束时间，仅在回测模式下有效
            - bt_stopped: 回测是否结束，仅在回测模式下有效。
            - last_trade: datetime.datetime, 最后一笔交易时间
            - available: float, `dt`日可用资金
            - market_value: `dt`日股票市值
            - pnl: `dt`盈亏(绝对值)
            - ppnl: 盈亏(百分比)，即pnl/principal
            - positions: 当前持仓，dtype为position_dtype的numpy structured array

        """
        dt = dt or self.last_trade_date or self.bt_start

        cash = self.get_cash(dt)
        assets = await self.get_assets(dt)

        return {
            "name": self.account_name,
            "principal": self.principal,
            "start": self.bt_start,
            "end": self.bt_end,
            "bt_stopped": self._bt_stopped,
            "last_trade": self.last_trade_date,
            "assets": assets,
            "available": cash,
            "market_value": assets - cash,
            "pnl": assets - self.principal,
            "ppnl": assets / self.principal - 1,
            "positions": self.get_position(dt),
        }

    def get_returns(
        self,
        start_date: Optional[datetime.date] = None,
        end_date: Optional[datetime.date] = None,
    ) -> NDArray:
        """求截止`end_date`时的每日回报

        Args:
            start_date: 计算回报的起始日期
            end_date : 计算回报的结束日期

        Returns:
            以百分比为单位的每日回报率,索引为对应日期
        """
        start = start_date or self.bt_start
        end = end_date or self.bt_end

        assert self.bt_start <= start <= end
        assert start <= end <= self.bt_end

        istart = np.argwhere(start >= self._assets["date"]).flatten()[-1]
        if istart > 0:
            istart -= 1
        iend = np.argwhere(end >= self._assets["date"]).flatten()[-1]
        if istart >= iend:
            raise TradeError(
                f"date range error: {start} - {end} contains no data", with_stack=True
            )

        # it's ok if iend + 1 > len(self._assets)
        assets = self._assets[istart : iend + 1]["assets"]
        return assets[1:] / assets[:-1] - 1

    @property
    def assets(self) -> float:
        """最新总资产。

        如果要获取历史上某天的总资产，请使用`get_assets`方法。
        """
        return self._assets[-1]["assets"]

    async def get_assets(self, date: Optional[datetime.date] = None) -> float:
        """查询某日的总资产

        当日总资产 = 当日可用资金 + 持仓市值
        本方法可用以临时查询，查询过程中不会修改现有资产表。

        Args:
            date: 查询哪一天的资产

        Returns:
            返回某一日的总资产

        """
        if date is None:
            return self._assets[-1]["assets"]

        last = self._assets[-1]["date"]
        if date > last:
            # 使用最后一天的持仓，last~date之间的收盘价计算每日市值，再加上现金
            # 因此这里不能使用_query_market_values
            filter = (self._positions["date"] == last) & (
                self._positions["security"] != None  # noqa: E711
            )

            secs = self._positions[filter]["security"]

            if len(secs) == 0:  # 无持仓
                return self._cash[-1]["cash"]

            feed = get_app_context().feed
            df_prices = await feed.batch_get_close_price_in_range(secs, last, date)

            df_shares = pd.DataFrame(data=self._positions[filter]).pivot(
                columns="security", index="date", values="shares"
            )

            # 2. df_shares * df_close then sum on columns => mv
            mv = df_prices.multiply(df_shares.iloc[0]).sum(axis=1)

            return mv.iloc[-1] + self._cash[-1]["cash"]
        else:
            pos = np.argwhere(date >= self._assets["date"]).flatten()
            if len(pos) > 0:
                return self._assets[pos[-1]]["assets"].item()
            else:  # 日期小于回测起始日
                return self.principal

    def _index_of(
        self, arr: np.ndarray, date: datetime.date, index: str = "date"
    ) -> int:
        """查找`arr`中其`index`字段等于`date`的索引

            注意数组中`date`字段取值必须惟一。

        Args:
            arr: numpy array, 需要存在`index`字段
            date: datetime.date, 查找的日期

        Returns:
            如果存在，返回索引，否则返回None
        """
        pos = np.argwhere(arr[index] == date).ravel()

        assert len(pos) <= 1, "date should be unique"
        if len(pos) == 0:
            return None

        return pos[0]

    @deprecated("deprecation since 0.5, use _forward_assets instead.")
    async def _calc_assets(self, date: datetime.date) -> Tuple[float, float, float]:
        """计算某日的总资产

        此函数不更新资产表，以避免资产表中留下空洞。比如：
        当前最后交易日为4月10日，4月17日发生一笔委卖，导致cash/position记录更新到4/17，但资产表仍然保持在4月10日，此时如果缓存该记录，将导致资产表中留下空洞。

        Args:
            date: 计算哪一天的资产

        Returns:
            返回总资产, 可用资金, 持仓市值
        """
        if date < self.bt_start:
            return self.principal, 0, 0

        if date > self.bt_end:
            raise BadParamsError(
                f"wrong date: {date}, date must be before {self.bt_end} or {arrow.now().date()}"
            )

        cash = self.get_cash(date)
        positions = self.get_position(date)
        # this also exclude empty entry (which security is None)
        heldings = positions[positions["shares"] > 0]["security"]

        market_value = 0
        if heldings.size > 0:
            feed = get_app_context().feed

            for sec in heldings:
                shares = positions[positions["security"] == sec]["shares"].item()
                price = await feed.get_close_price(sec, date)

                if price is not None:
                    market_value += shares * price
                else:
                    price = positions[positions["security"] == sec]["price"].item()
                    market_value += shares * price

        assets = cash + market_value

        return assets, cash, market_value

    @property
    def position(self) -> np.ndarray:
        """获取最后持仓

        如果要获取历史上某天的持仓，请使用`get_position`方法。
        如果当天个股曾有持仓，但被清仓，持仓表仍保留entry，但shares将置为空。如果当天没有任何持仓（不包括当天清空的情况），则会留一个`security`字段为None的空entry。

        Returns:
            返回dtype为[position_dtype][backtest.trade.datatypes.position_dtype]的numpy structure array
        """
        if self._positions.size == 0:
            return np.array([], dtype=position_dtype)

        last_day = self._positions[-1]["date"]
        filter = (self._positions["date"] == last_day) & (
            self._positions["security"] != None  # noqa: E711
        )
        result = self._positions[filter]

        return result[list(position_dtype.names)].astype(position_dtype)  # type: ignore

    def __str__(self):
        s = (
            f"账户：{self.account_name}:\n"
            + f"    总资产：{self.assets:,.2f}\n"
            + f"    本金：{self.principal:,.2f}\n"
            + f"    可用资金：{self.cash:,.2f}\n"
            + f"    持仓：{self.position}\n"
        )

        return s

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>{self}"

    async def _calendar_validation(self, bid_time: datetime.datetime):
        """更新和校准交易日期

        如果是回测模式，则在进入_bt_stopped状态时,还要完整计算一次assets,此后不再重复计算。

        Args:
            bid_time : 交易发生的时间
        """
        if self._bt_stopped:
            logger.warning("回测已结束，账号已冻结: %s, %s", bid_time, self.bt_end, date=bid_time)
            raise AccountStoppedError(bid_time, self.bt_end, with_stack=True)

        if bid_time.date() < self.bt_start:
            logger.warning(
                "委托时间超过回测开始时间: %s, %s", bid_time, self.bt_start, date=bid_time
            )
            msg = f"委托时间 {bid_time} 超过了回测开始时间 {self.bt_start}."
            raise BadParamsError(msg)

        if bid_time.date() > self.bt_end:
            logger.warning("委托时间超过回测结束时间: %s, %s", bid_time, self.bt_end, date=bid_time)
            msg = f"委托时间 {bid_time} 超过了回测结束时间 {self.bt_end}."
            raise BadParamsError(msg)

        if self._first_trade_time is None:
            self._first_trade_time = bid_time
        elif bid_time < self._first_trade_time:
            logger.warning(
                "委托时间必须递增出现: %s -> %s", self._first_trade_time, bid_time, date=bid_time
            )
            raise TimeRewindError(bid_time, self._first_trade_time, with_stack=True)

        if self._last_trade_time is None or bid_time >= self._last_trade_time:
            self._last_trade_time = bid_time
        else:
            logger.warning(
                "委托时间必须递增出现：%s -> %s", self._last_trade_time, bid_time, date=bid_time
            )
            raise TimeRewindError(bid_time, self._last_trade_time, with_stack=True)

        cash_end = self._cash[-1]["date"]
        pos_end = self._positions[-1]["date"]

        if cash_end != pos_end:
            msg = f"cash table {cash_end} is not synced with position table {pos_end}"
            logger.warning(msg)
            raise TradeError(msg, with_stack=True)

    async def buy(
        self,
        security: str,
        bid_price: Union[int, float],
        bid_shares: int,
        bid_time: datetime.datetime,
    ) -> Trade:
        """买入委托

        买入以尽可能实现委托为目标。如果可用资金不足，但能买入部分股票，则部分买入。

        如果bid_price为None，则使用涨停价买入。

        Args:
            security str: 证券代码
            bid_price float: 委托价格。如果为None，则为市价委托
            bid_shares int: 询买的股数
            bid_time datetime.datetime: 委托时间

        Returns:
            [Trade][backtest.trade.trade.Trade]对象
        """
        # 同一个账户，也可能出现并发的买单和卖单，这些操作必须串行化
        async with self.lock:
            return await self._buy(security, bid_price, bid_shares, bid_time)

    async def _buy(
        self,
        security: str,
        bid_price: float,
        bid_shares: Union[float, int],
        bid_time: datetime.datetime,
    ) -> Trade:
        entrustlog.info(
            f"{bid_time}\t{security}\t{bid_shares}\t{bid_price}\t{EntrustSide.BUY}"
        )
        assert (
            type(bid_time) is datetime.datetime
        ), f"{bid_time} is not type of datetime"

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
            "买入委托: %s %d %s, 单号：%s",
            security,
            bid_shares,
            bid_price,
            en.eid,
            date=bid_time,
        )

        self.entrusts[en.eid] = en

        _, buy_limit_price, sell_limit_price = await feed.get_trade_price_limits(
            security, bid_time.date()
        )

        bid_price = bid_price or buy_limit_price

        # 获取用以撮合的数据
        bars = await feed.get_price_for_match(security, bid_time)
        if bars.size == 0:
            logger.warning(
                "failed to match %s, no data at %s", security, bid_time, date=bid_time
            )
            raise NoDataForMatch(security, bid_time, with_stack=True)

        # 移除掉涨停和价格高于委买价的bar后，看还能买多少股
        bars = self._remove_for_buy(
            security, bid_time, bars, bid_price, buy_limit_price, sell_limit_price
        )

        # 将买入数限制在可用资金范围内
        shares_to_buy = min(
            bid_shares, self.cash // (bid_price * (1 + self.commission))
        )

        # 必须以手为单位买入，否则委托会失败
        shares_to_buy = shares_to_buy // 100 * 100
        if shares_to_buy < 100:
            logger.info("委买失败：%s, 资金(%s)不足购买1手。", security, self.cash, date=bid_time)
            raise CashError(
                self.account_name,
                max(100, shares_to_buy) * bid_price,
                self.cash,
                with_stack=True,
            )

        mean_price, filled, close_time = self._match_bid(bars, shares_to_buy)
        if filled == 0:
            raise VolumeNotMeet(security, bid_price, with_stack=True)

        return await self._after_buy(en, mean_price, filled, close_time)

    def _match_bid(
        self, bid_queue, shares_to_buy
    ) -> Tuple[float, float, datetime.datetime]:
        """计算此次买入的成交均价和成交量

        Args:
            bid_queue : 撮合数据
            shares_to_buy : 要买入的股数

        Returns:
            成交均价、可埋单股数和最后成交时间
        """
        c, v = bid_queue["price"], bid_queue["volume"]

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

    def _forward_unclosed_trades(self, dt: datetime.date):
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
        self._forward_unclosed_trades(date)

        self._unclosed_trades[date].append(tid)

    async def _after_buy(
        self, en: Entrust, price: float, filled: float, close_time: datetime.datetime
    ) -> Trade:
        """更新未成交表、现金表、持仓表和assets表

        Args:
            en : the trade just done
            price : _description_
            filled : _description_
            close_time : _description_

        Returns:
            成交记录
        """
        money = price * filled
        fee = math_round(money * self.commission, 2)

        trade = Trade(en.eid, en.security, price, filled, fee, en.side, close_time)
        self.trades[trade.tid] = trade
        self._update_unclosed_trades(trade.tid, close_time.date())
        await self._update_positions(trade, close_time.date())

        logger.info(
            "买入成交: %s (%d %.2f %.2f),委单号: %s, 成交号: %s",
            trade.security,
            filled,
            price,
            trade.fee,
            trade.eid,
            trade.tid,
            date=close_time,
        )
        tradelog.info(
            f"{en.bid_time.date()}\t{en.side}\t{en.security}\t{filled}\t{price}\t{fee}"
        )

        logger.info(
            "买入后持仓: \n%s",
            tabulate_numpy_array(
                self.get_position(close_time.date(), daily_position_dtype)
            ),
            date=close_time,
        )

        # 当发生新的买入时，现金表
        cash_change = -1 * (money + fee)
        self._update_cash(cash_change, close_time.date())
        await self._forward_assets(close_time.date())

        await emit.emit(E_BACKTEST, {"buy": jsonify(trade)})
        return trade

    def _update_cash(self, cash_change: float, date: datetime.date):
        """在买入、卖出之后，更新现金流表"""
        ipos = np.argwhere(self._cash["date"] == date).flatten()
        if len(ipos) != 1:
            raise IndexError("date not found in cash table. before_trade not called?")

        i = ipos.item()
        self._cash[i]["cash"] += cash_change

    async def _before_trade(self, bid_time: datetime.datetime):
        """交易前的准备工作

        在每次交易前，将_cash, _positionsg延展到当天，将_assets更新到前一天

        Args:
            bid_time: 委托时间

        Returns:
            无
        """
        logger.info("before trade", date=bid_time)
        await self._calendar_validation(bid_time)

        self._forward_cashtable(bid_time.date())
        await self._forward_positions(bid_time.date())

    def _forward_cashtable(self, end: datetime.date):
        """补齐现金表到end日"""
        # 现金表已是最新
        if end <= self._cash[-1]["date"]:
            return

        if end > self.bt_end:
            end = self.bt_end

        start = self._cash[-1]["date"]
        frames = tf.get_frames(start, end, FrameType.DAY)[1:]
        _, cash = self._cash[-1]

        recs = [(tf.int2date(date), cash) for date in frames]
        self._cash = np.concatenate((self._cash, np.array(recs, dtype=cash_dtype)))

    async def _forward_positions(self, end: datetime.date):
        """补齐持仓表到`end`日

        如果调用时，`end`日持仓表已存在，则不进行更新
        """
        if end <= self._positions[-1]["date"]:
            return

        if end > self.bt_end:
            end = self.bt_end

        start = self._positions[-1]["date"]
        if start >= end:
            logger.warning(
                "no need forward positions table, start %, end %", start, end
            )
            return

        # 注意 frames[0]已经存在持仓
        frames = [
            tf.int2date(frame) for frame in tf.get_frames(start, end, FrameType.DAY)
        ]

        feed = get_app_context().feed

        logger.info(
            "handling positions forward from %s to %s", frames[1], end, date=end
        )

        cur_position = self._positions[
            self._positions["date"] == self._positions[-1]["date"]
        ]

        # 已清空股票不需要展仓, issue 9
        last_held_position = cur_position[cur_position["shares"] != 0]

        if last_held_position.size == 0:
            empty = np.array(
                [(frame, None, 0, 0, 0) for frame in frames[1:]],
                dtype=daily_position_dtype,
            )
            self._positions = np.concatenate((self._positions, empty))
            return

        secs = last_held_position["security"].tolist()
        dr_info = await feed.get_dr_factor(secs, frames)

        for sec in secs:
            paddings = pd.DataFrame([], index=frames)
            paddings["security"] = sec
            rec = last_held_position[last_held_position["security"] == sec]
            paddings["shares"] = rec["shares"] * dr_info[sec]
            # 过了一天，所有股都变可售，除了除权股
            paddings["sellable"] = paddings["shares"].shift()
            paddings.iloc[0, 2] = rec["shares"].item()

            paddings["price"] = rec["price"] / dr_info[sec]

            adjust_shares = paddings["shares"].diff()
            for frame, adjust_share in adjust_shares[adjust_shares > 0].items():
                order_time = tf.combine_time(frame, 15)
                trade = Trade(
                    uuid.uuid4().hex,
                    sec,
                    paddings.loc[frame, "price"].item(),
                    adjust_share,
                    0,
                    EntrustSide.XDXR,
                    order_time,
                )
                self.trades[trade.tid] = trade
                self._update_unclosed_trades(trade.tid, order_time.date())
            self._positions = np.concatenate(
                (
                    self._positions,
                    paddings.iloc[1:]
                    .to_records(index=True)
                    .astype(self._positions.dtype),
                )
            )

    async def _update_positions(self, trade: Trade, bid_date: datetime.date):
        """更新持仓信息

        持仓信息为一维numpy数组，其类型为daily_position_dtype。如果某支股票在某日被清空，则当日持仓表保留记录，置shares为零，方便通过持仓表看出股票的进场出场时间，另一方面，如果不保留这条记录（而是删除），则在所有股票都被清空的情况下，会导致持仓表出现空洞，从而导致下一次交易时，误将更早之前的持仓记录复制到当日的持仓表中（在_before_trade中），而这些持仓实际上已经被清空。

        Args:
            trade: 交易信息
            bid_date: 买入/卖出日期
        """
        # delete empty records since we'll have at least one for bid_date
        if (
            self._positions[-1]["date"] == bid_date
            and self._positions[-1]["security"] is None
        ):
            self._positions = self._positions[:-1]

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
                if shares <= 0.1:
                    old_price = 0
                    shares = 0
                    sellable = 0
                self._positions[i] = (
                    bid_date,
                    trade.security,
                    shares,
                    sellable,
                    old_price,  # 卖出时成本不变，除非已清空
                )

        return

    async def _after_sell(
        self, en: Entrust, price: float, to_sell: float
    ) -> List[Trade]:
        """从positions中扣减股票、增加可用现金

        Args:
            en : 委卖单
            price : 成交均价
            filled : 回报的卖出数量

        Returns:
            成交记录列表
        """
        dt = en.bid_time.date()

        money = price * to_sell
        fee = math_round(money * self.commission, 2)

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
                    "卖出成交: %s (%d %.2f %.2f),委单号: %s, 成交号: %s",
                    en.security,
                    exit_trade.shares,
                    exit_trade.price,
                    exit_trade.fee,
                    en.eid,
                    exit_trade.tid,
                    date=exit_trade.time,
                )
                tradelog.info(
                    f"{en.bid_time.date()}\t{exit_trade.side}\t{exit_trade.security}\t{exit_trade.shares}\t{exit_trade.price}\t{exit_trade.fee}"
                )
                await self._update_positions(exit_trade, exit_trade.time.date())
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
            "卖出后持仓: \n%s",
            tabulate_numpy_array(self.get_position(dt, daily_position_dtype)),
            date=dt,
        )

        self._update_cash(refund, en.bid_time.date())
        await self._forward_assets(en.bid_time.date())

        await emit.emit(E_BACKTEST, {"sell": jsonify(exit_trades)})
        return exit_trades

    async def sell(
        self,
        security: str,
        bid_price: Union[None, float],
        bid_shares: float,
        bid_time: datetime.datetime,
    ) -> List[Trade]:
        """卖出委托

        Args:
            security str: 委托证券代码
            bid_price float: 出售价格，如果为None，则为市价委托
            bid_shares float: 询卖股数。注意我们不限制必须以100的倍数卖出。
            bid_time datetime.datetime: 委托时间

        Returns:
            成交记录列表,每个元素都是一个[Trade][backtest.trade.trade.Trade]对象

        """
        # 同一个账户，也可能出现并发的买单和卖单，这些操作必须串行化
        async with self.lock:
            return await self._sell(security, bid_price, bid_shares, bid_time)

    async def _sell(
        self,
        security: str,
        bid_price: Union[None, float],
        bid_shares: float,
        bid_time: datetime.datetime,
    ) -> List[Trade]:
        await self._before_trade(bid_time)

        feed = get_app_context().feed

        entrustlog.info(
            f"{bid_time}\t{security}\t{bid_shares}\t{bid_price}\t{EntrustSide.SELL}"
        )
        logger.info("卖出委托: %s %s %s", security, bid_price, bid_shares, date=bid_time)
        _, buy_limit_price, sell_limit_price = await feed.get_trade_price_limits(
            security, bid_time.date()
        )

        if bid_price is None:
            bid_type = BidType.MARKET
            bid_price = sell_limit_price
        else:
            bid_type = BidType.LIMIT

        # fill the order, get mean price
        bars = await feed.get_price_for_match(security, bid_time)
        if bars.size == 0:
            logger.warning(
                "failed to match: %s, no data at %s", security, bid_time, date=bid_time
            )
            raise NoDataForMatch(security, bid_time, with_stacks=True)

        bars = self._remove_for_sell(
            security, bid_time, bars, bid_price, sell_limit_price, buy_limit_price
        )

        shares_to_sell = self._get_sellable_shares(security, bid_shares, bid_time)
        if shares_to_sell == 0:
            logger.info("卖出失败: %s %s, 可用股数为0", security, bid_shares, date=bid_time)
            logger.info("%s", self.get_unclosed_trades(bid_time.date()), date=bid_time)
            raise PositionError(security, bid_time, with_stack=True)

        mean_price, filled, close_time = self._match_bid(bars, shares_to_sell)

        en = Entrust(
            security, EntrustSide.SELL, bid_shares, bid_price, bid_time, bid_type
        )

        logger.info(
            "委卖%s(%s), 成交%s股，均价%.2f",
            en.security,
            en.eid,
            filled,
            mean_price,
            date=close_time,
        )

        return await self._after_sell(en, mean_price, filled)

    def _get_sellable_shares(
        self, security: str, shares_asked: int, bid_time: datetime.datetime
    ) -> int:
        """在未完结的交易中寻找可卖股数(满足T+1条件)

        如果shares_asked与可售之间的差不足1股，则自动加上零头，确保可以卖完。

        Args:
            security: 证券代码
            shares_asked: 要求卖出数量
            bid_time: 委卖时间

        Returns:
            可卖股数
        """
        shares = 0
        for tid in self.get_unclosed_trades(bid_time.date()):
            t = self.trades[tid]
            if t.security == security and t.time.date() < bid_time.date():
                if t.side in (EntrustSide.BUY, EntrustSide.XDXR):
                    assert t.closed is False
                shares += t._unsell

        if shares - shares_asked < 100:
            return shares
        return min(shares_asked, shares)

    def _remove_for_buy(
        self,
        security: str,
        order_time: datetime.datetime,
        bars: np.ndarray,
        price: float,
        buy_limit_price: float,
        sell_limit_price: float,
    ) -> np.ndarray:
        """
        去掉已达到涨停时的分钟线，或者价格高于买入价的bars，并且，如果当天有跌停价，将该处的成交量修改为无穷大，以便后面做撮合时，可以无限量买入
        """
        reach_limit = array_price_equal(bars["price"], buy_limit_price)
        bars = bars[(~reach_limit)]

        if bars.size == 0:
            raise BuylimitError(security, order_time, with_stack=True)

        bars = bars[(bars["price"] <= price)]
        if bars.size == 0:
            raise PriceNotMeet(security, price, order_time, with_stack=True)

        where_sell_stop = array_price_equal(bars["price"], sell_limit_price)
        bars["volume"][where_sell_stop] = 1e20
        return bars

    def _remove_for_sell(
        self,
        security: str,
        order_time: datetime.datetime,
        bars: np.ndarray,
        price: float,
        sell_limit_price: float,
        buy_limit_price: float,
    ) -> np.ndarray:
        """去掉当前价格低于price，或者已经达到跌停时的bars,这些bars上无法成交

        如果存在涨停的bar，这些bar上的成交量将放大到1e20，以便后面模拟允许涨停板上无限卖出的行为。

        """
        reach_limit = array_price_equal(bars["price"], sell_limit_price)
        bars = bars[(~reach_limit)]

        if bars.size == 0:
            raise SellLimitError(security, order_time, with_stack=True)

        bars = bars[(bars["price"] >= price)]
        if bars.size == 0:
            raise PriceNotMeet(security, price, order_time, with_stack=True)

        where_buy_stop = array_price_equal(bars["price"], buy_limit_price)
        bars["volume"][where_buy_stop] = 1e20
        return bars

    async def metrics(
        self,
        start: Optional[datetime.date] = None,
        end: Optional[datetime.date] = None,
        baseline: Optional[str] = "399300.XSHE",
    ) -> Dict:
        """获取指定时间段的账户指标

        Args:
            start: 开始时间
            end: 结束时间
            baseline: 参考标的

        Returns:
            Dict: 指标字典，其key为

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
        if not self._bt_stopped:
            raise TradeError("call stop_backtest before invoke this")

        try:
            rf = cfg.metrics.risk_free_rate / cfg.metrics.annual_days
        except Exception:
            rf = 0

        start = max(start or self.bt_start, self.first_trade_date or self.bt_start)
        end = min(self.last_trade_date or self.bt_end, end or self.bt_end)

        tx = []
        logger.info("%s tx in total", len(self.transactions))
        for t in self.transactions:
            if t.entry_time.date() >= start and t.exit_time.date() <= end:
                tx.append(t)
            else:
                logger.info(
                    "tx %s not in range, start: %s, end: %s",
                    t.sec,
                    t.entry_time,
                    t.exit_time,
                )

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

        total_profit = self._assets[-1]["assets"] - self._assets[0]["assets"]

        returns = self.get_returns(start, end)
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
                    "total_profit_rate": cum_returns_final(returns),
                    "win_rate": np.count_nonzero(returns > 0) / len(returns),
                    "mean_return": np.mean(returns).item(),
                    "sharpe": sharpe_ratio(returns, rf),
                    "sortino": sortino_ratio(returns, rf),
                    "calmar": calmar_ratio(returns),
                    "max_drawdown": max_drawdown(returns),
                    "annual_return": annual_return(returns),
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
            "total_profit_rate": total_profit / self.principal,
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

    async def stop_backtest(self):
        """停止回测，冻结指标"""
        self._bt_stopped = True
        self._forward_cashtable(self.bt_end)
        await self._forward_positions(self.bt_end)
        await self._forward_assets(self.bt_end)

    def bills(self) -> dict:
        if not self._bt_stopped:
            raise TradeError("call `bt_stopped` first!")

        results = {}
        results["tx"] = self.transactions
        results["trades"] = self.trades
        results["positions"] = self._positions

        results["assets"] = self._assets
        return results
