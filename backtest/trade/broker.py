"""
Broker是一个交易代理。每一个交易代理对应一个账户，记录了该账户下的交易记录、每日持仓记录和每日市值记录等数据，并提供交易撮合的具体实现。
"""
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
from omicron.extensions.np import numpy_append_fields
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame as tf
from pyemit import emit

from backtest.common.errors import AccountError, BadParameterError, EntrustError
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

cfg = cfg4py.get_instance()
logger = logging.getLogger(__name__)
entrustlog = logging.getLogger("entrust")
tradelog = logging.getLogger("trade")


class Broker:
    def __init__(
        self,
        account_name: str,
        principal: float,
        commission: float,
        bt_start: datetime.date = None,
        bt_end: datetime.date = None,
    ):
        """创建一个Broker对象

        Args:
            account_name : 账号/策略名
            principal : 初始本金
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
        self._last_trade_time: datetime.datetime = None
        self._first_trade_time: datetime.datetime = None

        self.account_name = account_name
        self.commission = commission

        # 初始本金
        self.principal = principal
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
        # self._lock is not pickable
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
            return self.principal

        return self._cash[-1]["cash"].item()

    @property
    def account_start_date(self) -> datetime.date:
        if self.mode == "bt":
            return self.bt_start
        else:
            return (
                None
                if self._first_trade_time is None
                else self._first_trade_time.date()
            )

    @property
    def account_end_date(self) -> datetime.date:
        if self.mode == "bt":
            return self.bt_stop
        else:
            return (
                None if self._last_trade_time is None else self._last_trade_time.date()
            )

    @property
    def last_trade_date(self):
        return None if self._last_trade_time is None else self._last_trade_time.date()

    @property
    def first_trade_date(self):
        return None if self._first_trade_time is None else self._first_trade_time.date()

    def get_cash(self, dt: datetime.date) -> float:
        """获取`dt`当天的可用资金

        在查询时，如果`dt`小于首次交易日，则返回空，否则，如果当日无数据，将从上一个有数据之日起，进行补齐填充。
        Args:
            dt (datetime.date): 日期

        Returns:
            float: 某日可用资金
        """
        if self._cash.size == 0:
            return self.principal

        if dt > self._cash[-1]["date"]:
            return self._cash[-1]["cash"].item()
        elif dt < self._cash[0]["date"]:
            return self.principal

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

    async def recalc_assets(self, end: datetime.date = None):
        """重新计算账户的每日资产

        计算完成后，资产表将包括从账户开始前一日，到`end`日的资产数据。从账户开始前一日起，是为了方便计算首个交易日的收益。

        Args:
            end: 计算到哪一天的资产，默认为空，即计算到最后一个交易日（非回测），或者回测结束日。

        """
        if end is None:
            if self.mode != "bt":  # 非回测下计算到当下
                end = arrow.now().date()
            else:  # 回测时计算到bt_stop
                end = self.bt_stop

        # 把期初资产加进来
        if self._assets.size == 0:
            start = self.account_start_date
            if start is None:
                return np.array([], dtype=rich_assets_dtype)

            _before_start = tf.day_shift(start, -1)
            self._assets = np.array(
                [(_before_start, self.principal)], dtype=assets_dtype
            )

        start = tf.day_shift(self._assets[-1]["date"], 1)
        if start >= end:
            return

        for frame in tf.get_frames(start, end, FrameType.DAY):
            date = tf.int2date(frame)
            await self._calc_assets(date)

    async def info(self, dt: datetime.date = None) -> Dict:
        """`dt`日的账号相关信息

        Returns:
            Dict: 账号相关信息：

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
        dt = dt or self.last_trade_date

        cash = self.get_cash(dt)
        assets = await self.get_assets(dt)

        return {
            "name": self.account_name,
            "principal": self.principal,
            "start": self.account_start_date,
            "end": self.bt_stop,
            "bt_stopped": self._bt_stopped,
            "last_trade": self.last_trade_date,
            "assets": assets,
            "available": cash,
            "market_value": assets - cash,
            "pnl": assets - self.principal,
            "ppnl": assets / self.principal - 1,
            "positions": self.get_position(dt),
        }

    async def get_returns(
        self, start_date: datetime.date = None, end_date: datetime.date = None
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

        if not self._bt_stopped:
            await self.recalc_assets()

        assets = self._assets[
            (self._assets["date"] >= _start) & (self._assets["date"] <= end)
        ]

        if assets.size == 0:
            raise ValueError(f"date range error: {start} - {end} contains no data")

        return assets["assets"][1:] / assets["assets"][:-1] - 1

    @property
    def assets(self) -> float:
        """当前总资产。

        如果要获取历史上某天的总资产，请使用`get_assets`方法。
        """
        if self._assets.size == 0:
            return self.principal
        else:
            return self._assets[-1]["assets"]

    async def get_assets(self, date: datetime.date) -> float:
        """查询某日的总资产

        当日总资产 = 当日可用资金 + 持仓市值

        Args:
            date: 查询哪一天的资产

        Returns:
            返回某一日的总资产

        """
        if self._assets.size == 0:
            return self.principal

        if date is None:
            return self._assets[-1]["assets"]

        result = self._assets[self._assets["date"] == date]
        if result.size == 1:
            return result["assets"].item()

        assets, *_ = await self._calc_assets(date)
        return assets

    def _index_of(self, arr: np.ndarray, date: datetime.date) -> int:
        """查找`arr`中其`date`字段等于`date`的索引

            注意数组中`date`字段取值必须惟一。

        Args:
            arr: numpy array, 需要存在`date`字段
            date: datetime.date, 查找的日期

        Returns:
            如果存在，返回索引，否则返回None
        """
        pos = np.argwhere(arr["date"] == date).ravel()

        assert len(pos) <= 1, "date should be unique"
        if len(pos) == 0:
            return None

        return pos[0]

    async def _calc_assets(self, date: datetime.date) -> Tuple[float]:
        """计算某日的总资产，并缓存

        Args:
            date : 计算哪一天的资产

        Returns:
            返回总资产, 可用资金, 持仓市值
        """
        if date < self.account_start_date:
            return self.principal, 0, 0

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

        i = self._index_of(self._assets, date)
        if i is None:
            self._assets = np.append(
                self._assets, np.array([(date, assets)], dtype=assets_dtype)
            )
        else:
            # don't use self._assets[self._assets["date"] == date], this always return copy
            self._assets[i]["assets"] = assets

        return assets, cash, market_value

    @property
    def position(self) -> np.ndarray:
        """获取当前持仓

        如果要获取历史上某天的持仓，请使用`get_position`方法。

        Returns:
            返回dtype为[position_dtype][backtest.trade.datatypes.position_dtype]的numpy structure array
        """
        if self._positions.size == 0:
            return np.array([], dtype=position_dtype)

        last_day = self._positions[-1]["date"]
        result = self._positions[self._positions["date"] == last_day]

        return result[list(position_dtype.names)].astype(position_dtype)

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
        if self.mode == "bt" and self._bt_stopped:
            logger.warning("委托时间超过回测结束时间: %s, %s", bid_time, self.bt_stop)
            raise AccountError(f"下单时间为{bid_time},而账户已于{self.bt_stop}冻结。")

        if self._first_trade_time is None:
            self._first_trade_time = bid_time
        elif bid_time < self._first_trade_time:
            logger.warning("委托时间必须递增出现: %s -> %s", self._first_trade_time, bid_time)
            raise EntrustError(
                EntrustError.TIME_REWIND,
                time=bid_time,
                last_trade_time=self._first_trade_time,
            )

        if self._last_trade_time is None or bid_time >= self._last_trade_time:
            self._last_trade_time = bid_time
        else:
            logger.warning("委托时间必须递增出现：%s -> %s", self._last_trade_time, bid_time)
            raise EntrustError(
                EntrustError.TIME_REWIND,
                time=bid_time,
                last_trade_time=self._last_trade_time,
            )

        if self.mode == "bt" and bid_time.date() > self.bt_stop:
            self._bt_stopped = True
            await self.recalc_assets()
            logger.warning("委托时间超过回测结束时间: %s, %s", bid_time, self.bt_stop)
            raise AccountError(f"下单时间为{bid_time},而账户已于{self.bt_stop}冻结。")

    async def buy(self, *args, **kwargs) -> Trade:
        """买入委托

        买入以尽可能实现委托为目标。如果可用资金不足，但能买入部分股票，则部分买入。

        如果bid_price为None，则使用涨停价买入。

        Args:
            security str: 证券代码
            bid_price float: 委托价格。如果为None，则为市价委托
            bid_shares int: 询买的股数
            bid_time datetime.datetime: 委托时间
            request_id str: 请求ID

        Returns:
            [Trade][backtest.trade.trade.Trade]对象
        """
        # 同一个账户，也可能出现并发的买单和卖单，这些操作必须串行化
        async with self.lock:
            return await self._buy(*args, **kwargs)

    async def _buy(
        self,
        security: str,
        bid_price: float,
        bid_shares: int,
        bid_time: datetime.datetime,
    ) -> Dict:
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
        bars = await feed.get_price_for_match(security, bid_time)
        if bars.size == 0:
            logger.warning("failed to match %s, no data at %s", security, bid_time)
            raise EntrustError(
                EntrustError.NODATA_FOR_MATCH, security=security, time=bid_time
            )

        # 移除掉涨停和价格高于委买价的bar后，看还能买多少股
        bars = self._remove_for_buy(
            security, bid_time, bars, bid_price, buy_limit_price
        )

        # 将买入数限制在可用资金范围内
        shares_to_buy = min(
            bid_shares, self.cash // (bid_price * (1 + self.commission))
        )

        # 必须以手为单位买入，否则委托会失败
        shares_to_buy = shares_to_buy // 100 * 100
        if shares_to_buy < 100:
            logger.info("委买失败：%s(%s), 资金(%s)不足", security, self.cash, en.eid)
            raise EntrustError(
                EntrustError.NO_CASH,
                account=self.account_name,
                required=100 * bid_price,
                available=self.cash,
            )

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
    ) -> Trade:
        """生成trade,更新交易、持仓和assets

        Args:
            en : _description_
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
            "买入成交(%s): %s (%d %.2f %.2f),委单号: %s, 成交号: %s",
            close_time,
            en.security,
            filled,
            price,
            fee,
            en.eid,
            trade.tid,
        )
        tradelog.info(
            f"{en.bid_time.date()}\t{en.side}\t{en.security}\t{filled}\t{price}\t{fee}"
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
        await self._update_assets(cash_change, close_time)

        await emit.emit(E_BACKTEST, {"buy": jsonify(trade)})
        return trade

    async def _before_trade(self, bid_time: datetime.datetime):
        """交易前的准备工作

        在每次交易前，补齐每日现金数据和持仓数据到`bid_time`，更新账户生命期等。

        Args:
            bid_time: 委托时间

        Returns:
            无
        """
        await self._calendar_validation(bid_time)

        # 补齐可用将资金表
        if self._cash.size == 0:
            start = tf.day_shift(self.account_start_date, -1)
            end = bid_time.date()
            frames = tf.get_frames(start, end, FrameType.DAY)
            _cash = [(tf.int2date(frame), self.principal) for frame in frames]
            self._cash = np.array(_cash, dtype=cash_dtype)
        else:
            last_dt, cash = self._cash[-1]

            frames = tf.get_frames(last_dt, bid_time, FrameType.DAY)[1:]
            if frames.size > 0:
                recs = [(tf.int2date(date), cash) for date in frames]

                self._cash = np.concatenate(
                    (self._cash, np.array(recs, dtype=cash_dtype))
                )

        # 补齐持仓表(需要处理复权)
        feed = get_app_context().feed

        if self._positions.size == 0:
            return

        last_dt = self._positions[-1]["date"]

        frames = tf.get_frames(last_dt, bid_time, FrameType.DAY)[1:]
        if frames.size == 0:
            return

        data = self._positions.tolist()

        last_day_position = self._positions[self._positions["date"] == last_dt]
        dr_baseline_dt = last_dt
        for frame in frames:
            # 将last_dt的持仓数据补齐到frame日期的持仓数据中
            copy = last_day_position.copy()

            cur_date = tf.int2date(frame)
            copy["date"] = cur_date
            # 延后一日后，持仓全部可用
            copy["sellable"] = copy["shares"]
            data.extend(copy)

            # 如果当日有复权，需要将除权除息损益计入现金表
            for i, sec in enumerate(copy["security"]):
                dr = await feed.calc_xr_xd(
                    sec, dr_baseline_dt, cur_date, copy["shares"][i]
                )

                if dr > 0:
                    logger.info("%s于%s发生除权除息:%s", sec, cur_date, dr)
                dr_baseline_dt = cur_date
                _index = np.argwhere(self._cash["date"] >= cur_date).flatten()
                if _index.size > 0:
                    _index = _index[0]
                    self._cash[_index:]["cash"] += dr

        self._positions = np.array(data, dtype=daily_position_dtype)

    async def _update_positions(self, trade: Trade, bid_date: datetime.date):
        """更新持仓信息

        持仓信息为一维numpy数组，其类型为daily_position_dtype。如果某支股票在某日被清空，则当日持仓表保留记录，置shares为零，方便通过持仓表看出股票的进场出场时间，另一方面，如果不保留这条记录（而是删除），则在所有股票都被清空的情况下，会导致持仓表出现空洞，从而导致下一次交易时，误将更早之前的持仓记录复制到当日的持仓表中（在_before_trade中），而这些持仓实际上已经被清空。

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

    async def _update_assets(self, cash_change: float, bid_time: datetime.datetime):
        """更新当前资产（含持仓）

        在每次资产变动时进行计算和更新，并对之前的资产表进行补全。

        Args:
            cash_change : 变动的现金
            bid_time: 委托时间
        """
        logger.info("cash change: %s", cash_change)

        # 补齐资产表到上一个交易日
        if self._assets.size == 0:
            _before_start = tf.day_shift(self.account_start_date, -1)
            self._assets = np.array(
                [(_before_start, self.principal)], dtype=assets_dtype
            )

        start = tf.day_shift(self._assets[-1]["date"], 1)
        end = tf.day_shift(bid_time, -1)
        if start < end:
            await self.recalc_assets(end)

        bid_time = bid_time.date()

        # _before_trade应该已经为当日交易准备好了可用资金数据
        assert self._cash[-1]["date"] == bid_time
        self._cash[-1]["cash"] += cash_change

        assets, cash, mv = await self._calc_assets(bid_time)

        info = np.array(
            [(bid_time, assets, cash, mv, cash_change)],
            dtype=[
                ("date", "O"),
                ("assets", float),
                ("cash", float),
                ("market value", float),
                ("change", float),
            ],
        )
        logger.info("\n%s", tabulate_numpy_array(info))

    async def _fill_sell_order(
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
                    "卖出成交(%s): %s (%d %.2f %.2f),委单号: %s, 成交号: %s",
                    exit_trade.time,
                    en.security,
                    exit_trade.shares,
                    exit_trade.price,
                    exit_trade.fee,
                    en.eid,
                    exit_trade.tid,
                )
                tradelog.info(
                    f"{en.bid_time.date()}\t{exit_trade.side}\t{exit_trade.security}\t{exit_trade.shares}\t{exit_trade.price}\t{exit_trade.fee}"
                )
                await self._update_positions(exit_trade, exit_trade.time)
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

        await self._update_assets(refund, en.bid_time)

        await emit.emit(E_BACKTEST, {"sell": jsonify(exit_trades)})
        return exit_trades

    async def sell(self, *args, **kwargs) -> List[Trade]:
        """卖出委托

        Args:
            security: 委托证券代码
            price: 出售价格，如果为None，则为市价委托
            bid_shares: 询卖股数
            bid_time: 委托时间

        Returns:
            成交记录列表,每个元素都是一个[Trade][backtest.trade.trade.Trade]对象

        """
        # 同一个账户，也可能出现并发的买单和卖单，这些操作必须串行化
        async with self.lock:
            return await self._sell(*args, **kwargs)

    async def _sell(
        self,
        security: str,
        bid_price: float,
        bid_shares: int,
        bid_time: datetime.datetime,
    ) -> List[Trade]:
        await self._before_trade(bid_time)

        feed = get_app_context().feed

        entrustlog.info(
            f"{bid_time}\t{security}\t{bid_shares}\t{bid_price}\t{EntrustSide.SELL}"
        )
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
        bars = await feed.get_price_for_match(security, bid_time)
        if bars.size == 0:
            logger.warning("failed to match: %s, no data at %s", security, bid_time)
            raise EntrustError(
                EntrustError.NODATA_FOR_MATCH, security=security, time=bid_time
            )

        bars = self._remove_for_sell(
            security, bid_time, bars, bid_price, sell_limit_price
        )

        c, v = bars["price"], bars["volume"]

        cum_v = np.cumsum(v)

        shares_to_sell = self._get_sellable_shares(security, bid_shares, bid_time)
        if shares_to_sell == 0:
            logger.info("卖出失败: %s %s %s, 可用股数为0", security, bid_shares, bid_time)
            raise EntrustError(
                EntrustError.NO_POSITION, security=security, time=bid_time
            )

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
        self,
        security: str,
        order_time: datetime.datetime,
        bars: np.ndarray,
        price: float,
        limit_price: float,
    ) -> np.ndarray:
        """
        去掉已达到涨停时的分钟线，或者价格高于买入价的bars
        """
        reach_limit = array_price_equal(bars["price"], limit_price)
        bars = bars[(~reach_limit)]

        if bars.size == 0:
            raise EntrustError(
                EntrustError.REACH_BUY_LIMIT, security=security, time=order_time
            )

        bars = bars[(bars["price"] <= price)]
        if bars.size == 0:
            raise EntrustError(
                EntrustError.PRICE_NOT_MEET,
                security=security,
                time=order_time,
                entrust=price,
            )

        return bars

    def _remove_for_sell(
        self,
        security: str,
        order_time: datetime.datetime,
        bars: np.ndarray,
        price: float,
        limit_price: float,
    ) -> np.ndarray:
        """去掉当前价格低于price，或者已经达到跌停时的bars,这些bars上无法成交"""
        reach_limit = array_price_equal(bars["price"], limit_price)
        bars = bars[(~reach_limit)]

        if bars.size == 0:
            raise EntrustError(
                EntrustError.REACH_SELL_LIMIT, security=security, time=order_time
            )

        bars = bars[(bars["price"] >= price)]
        if bars.size == 0:
            raise EntrustError(
                EntrustError.PRICE_NOT_MEET, security=security, entrust=price
            )

        return bars

    def freeze(self):
        """冻结账户，停止接收新的委托"""
        self._bt_stopped = True

    async def metrics(
        self,
        start: datetime.date = None,
        end: datetime.date = None,
        baseline: str = None,
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

        if not self._bt_stopped:
            await self.recalc_assets()

        # 当计算[start, end]之间的盈亏时，我们实际上要多取一个交易日，即start之前一个交易日的资产数据
        _start = tf.day_shift(start, -1)
        total_profit = await self.get_assets(end) - await self.get_assets(_start)

        returns = await self.get_returns(start, end)
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
