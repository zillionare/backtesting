import datetime
import os
import unittest

import cfg4py
import numpy as np
import omicron
from omicron.models.timeframe import TimeFrame as tf
from sanic import Sanic

from backtest.common.helper import get_app_context
from backtest.config import get_config_dir
from backtest.feed.filefeed import FileFeed
from backtest.trade.broker import Broker
from backtest.trade.trade import Trade
from backtest.trade.types import (
    BidType,
    Entrust,
    EntrustError,
    EntrustSide,
    position_dtype,
)
from tests import data_dir

app = Sanic("backtest")


class BrokerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        global app
        cfg4py.init(get_config_dir())

        try:
            await omicron.init()
        except Exception:
            tf.service_degrade()

        self.ctx = get_app_context()
        match_bars_file = os.path.join(data_dir(), "bars_1m.pkl")
        price_limits_file = os.path.join(data_dir(), "limits.pkl")
        self.ctx.feed = FileFeed(match_bars_file, price_limits_file)
        await self.ctx.feed.init()

        return await super().asyncSetUp()

    def _check_positions(self, exp, actual):
        self.assertSetEqual(set(exp["security"]), set(actual["security"]))

        for sec in exp["security"]:
            a = exp[exp["security"] == sec]
            b = actual[actual["security"] == sec]
            self.assertAlmostEqual(a["shares"][0], b["shares"][0], 2)
            self.assertAlmostEqual(a["cost"][0], b["cost"][0], 2)

    def _check_order_result(self, actual, status, sec, price, shares, commission):
        self.assertEqual(actual["status"], status)

        if isinstance(sec, set):
            self.assertSetEqual(set([v["security"] for v in actual["data"]]), sec)
        else:
            self.assertEqual(actual["data"]["security"], sec)

        # exit price would be same
        if isinstance(actual["data"], list):
            for v in actual["data"]:
                self.assertAlmostEqual(v["price"], price, 2)

            sum_shares = np.sum([v["shares"] for v in actual["data"]])
            self.assertEqual(sum_shares, shares)
            sum_fee = np.sum([v["fee"] for v in actual["data"]])
            self.assertEqual(sum_fee, price * shares * commission)
        else:
            self.assertAlmostEqual(actual["data"]["price"], price, 2)

            self.assertEqual(actual["data"]["shares"], shares)
            self.assertAlmostEqual(
                actual["data"]["fee"], price * shares * commission, 2
            )

    def _check_balance(self, broker, cash, assets, positions: np.ndarray):
        self.assertAlmostEqual(broker.cash, cash)
        self.assertAlmostEqual(broker.assets, assets, 2)
        self.assertEqual(len(broker.positions), len(positions))

        self._check_positions(positions, broker.positions)

    async def test_buy(self):
        global bars

        hljh, capital, commission = "002537.XSHE", 1e10, 1e-4
        broker = Broker("test", capital, commission)

        # 委买部分成交
        result = await broker.buy(
            hljh,
            9.43,
            1e9,  # total available shares: 81_840_998
            datetime.datetime(2022, 3, 10, 9, 35),
        )

        price1, shares1, close_price_of_the_day = 9.324918623206482, 29265100.0, 9.68
        self._check_order_result(
            result, EntrustError.PARTIAL_SUCCESS, hljh, price1, shares1, commission
        )

        spend = price1 * shares1 * (1 + commission)
        cash = broker.capital - spend

        mv = shares1 * close_price_of_the_day  # market_value
        assets = cash + mv
        positions = np.array([(hljh, shares1, price1)], dtype=position_dtype)
        self._check_balance(broker, cash, assets, positions)

        # 委买当笔即全部成交
        start_cash = broker.cash  # 9240417581.183184

        result = await broker.buy(
            hljh,
            9.43,
            1e5,
            datetime.datetime(2022, 3, 10, 9, 35),
        )

        price2, shares2, close_price_of_the_day = 9.12, 1e5, 9.68

        self._check_order_result(
            result, EntrustError.SUCCESS, hljh, price2, shares2, commission
        )

        shares = shares1 + shares2
        price = (price1 * shares1 + price2 * shares2) / shares
        positions = np.array([(hljh, shares, price)], dtype=position_dtype)

        cash = start_cash - price2 * shares2 * (1 + commission)
        assets = cash + shares * close_price_of_the_day

        self._check_balance(broker, cash, assets, positions)

        # 买入时已经涨停
        result = await broker.buy(
            hljh, 9.68, 10e4, datetime.datetime(2022, 3, 10, 14, 33)
        )

        self.assertEqual(result["status"], EntrustError.REACH_BUY_LIMIT)

        # 资金不足,委托失败
        broker.cash = 100
        result = await broker.buy(
            hljh, 9.43, 10e4, datetime.datetime(2022, 3, 10, 9, 35)
        )

        self.assertEqual(result["status"], EntrustError.NO_CASH)

    async def test_get_positions(self):
        broker = Broker("test", 1e10, 1e-4)

        self.assertEqual(0, len(broker.get_positions(datetime.date(2022, 3, 3))))

        trade = Trade(
            1,
            "002537.XSHE",
            1.0,
            100,
            5,
            EntrustSide.BUY,
            datetime.datetime(2022, 3, 3),
        )
        broker.trades[trade.tid] = trade
        broker._append_unclosed_trades(trade.tid, datetime.date(2022, 3, 3))

        positions = broker.get_positions(datetime.date(2022, 3, 3))
        self.assertEqual(1, len(positions))
        self.assertTupleEqual(("002537.XSHE", 100, 0, 1.0), positions[0].tolist())

        positions = broker.get_positions(datetime.date(2022, 3, 7))
        self.assertEqual(3, len(broker._unclosed_trades))
        self.assertEqual(1, len(positions))
        self.assertTupleEqual(("002537.XSHE", 100, 100, 1.0), positions[0].tolist())

        trade = Trade(
            2,
            "603717.XSHG",
            1.0,
            1000,
            50,
            EntrustSide.BUY,
            datetime.datetime(2022, 3, 8),
        )
        broker.trades[trade.tid] = trade
        broker._append_unclosed_trades(trade.tid, datetime.date(2022, 3, 8))

        trade = Trade(
            3,
            "603717.XSHG",
            2.0,
            1000,
            70,
            EntrustSide.BUY,
            datetime.datetime(2022, 3, 8),
        )
        broker.trades[trade.tid] = trade
        broker._append_unclosed_trades(trade.tid, datetime.date(2022, 3, 9))

        self.assertEqual(5, len(broker._unclosed_trades))
        positions = broker.get_positions(datetime.date(2022, 3, 9))

        self.assertEqual(2, len(positions))
        if positions[0][0] == "603717.XSHG":
            self.assertTupleEqual(
                ("603717.XSHG", 2000, 2000, 1.5), positions[0].tolist()
            )
        else:
            self.assertTupleEqual(("002537.XSHE", 100, 100, 1), positions[0].tolist())

    async def test_current_positions(self):
        broker = Broker("test", 1e10, 1e-4)

        trade = Trade(
            1, "002537.XSHE", 1.0, 100, 5, EntrustSide.BUY, datetime.date(2022, 3, 3)
        )
        broker.trades[trade.tid] = trade
        broker._append_unclosed_trades(trade.tid, datetime.date(2022, 3, 3))

        positions = broker.positions
        self.assertEqual(1, len(positions))
        self.assertTupleEqual(("002537.XSHE", 100, 1.0), positions[0].tolist())

        trade = Trade(
            2, "603717.XSHG", 1.0, 1000, 50, EntrustSide.BUY, datetime.date(2022, 3, 8)
        )
        broker.trades[trade.tid] = trade
        broker._append_unclosed_trades(trade.tid, datetime.date(2022, 3, 8))

        trade = Trade(
            3, "603717.XSHG", 2.0, 1000, 70, EntrustSide.BUY, datetime.date(2022, 3, 8)
        )
        broker.trades[trade.tid] = trade
        broker._append_unclosed_trades(trade.tid, datetime.date(2022, 3, 9))

        positions = broker.positions

        self.assertEqual(2, len(positions))
        if positions[0][0] == "603717.XSHG":
            self.assertTupleEqual(("603717.XSHG", 2000, 1.5), positions[0].tolist())
        else:
            self.assertTupleEqual(("603717.XSHG", 2000, 1.5), positions[1].tolist())

    async def test_get_unclosed_trades(self):
        broker = Broker("test", 1e10, 1e-4)

        self.assertEqual(0, len(broker.get_unclosed_trades(datetime.date(2022, 3, 3))))

        broker._append_unclosed_trades(0, datetime.date(2022, 3, 3))
        self.assertListEqual([0], broker.get_unclosed_trades(datetime.date(2022, 3, 3)))

        self.assertListEqual([0], broker.get_unclosed_trades(datetime.date(2022, 3, 4)))
        self.assertEqual(2, len(broker._unclosed_trades))

    async def test_append_unclosed_trades(self):
        broker = Broker("test", 1e10, 1e-4)

        for i, dt in enumerate(
            [
                datetime.date(2022, 3, 3),
                datetime.date(2022, 3, 8),
                datetime.date(2022, 3, 9),
                datetime.date(2022, 3, 10),
            ]
        ):
            broker._append_unclosed_trades(i, dt)

        self.assertEqual(6, len(broker._unclosed_trades))
        self.assertListEqual([0], broker._unclosed_trades[datetime.date(2022, 3, 3)])
        self.assertListEqual(
            [0, 1, 2, 3], broker._unclosed_trades[datetime.date(2022, 3, 10)]
        )

    async def test_sell(self):
        broker = Broker("test", 1e6, 1e-4)
        tyst, hljh = "603717.XSHG", "002537.XSHE"

        await broker.buy(tyst, 14.84, 500, datetime.datetime(2022, 3, 7, 9, 41))
        await broker.buy(tyst, 14.79, 1000, datetime.datetime(2022, 3, 8, 14, 8))
        await broker.buy(hljh, 8.95, 1000, datetime.datetime(2022, 3, 9, 9, 40))
        await broker.buy(hljh, 9.09, 1000, datetime.datetime(2022, 3, 10, 9, 33))

        price, shares, time = 12.98, 1100, datetime.datetime(2022, 3, 10, 9, 35)

        # 可用余额足够，买单足够，close部分
        result = await broker.sell(tyst, price, shares, time)

        self.assertEqual(6, len(broker.trades))
        self.assertEqual(4, len(broker._unclosed_trades))
        exit_price, sold_shares = (13.57, 1100)
        self._check_order_result(
            result,
            EntrustError.SUCCESS,
            {tyst},
            exit_price,
            sold_shares,
            broker.commission,
        )

        pos = np.array([(tyst, 400, 14.79), (hljh, 2000, 9.02)], position_dtype)
        self._check_positions(broker.positions, pos)
        self.assertAlmostEqual(998964.975, broker.assets, 2)
        self.assertAlmostEqual(974672.975, broker.cash, 2)

        # 可用余额不足: 尝试卖出当天买入的部分
        bid_price, bid_shares, bid_time = (
            14.3,
            400,
            datetime.datetime(2022, 3, 7, 14, 26),
        )
        result = await broker.sell(tyst, bid_price, bid_shares, bid_time)
        self.assertEqual(EntrustError.NO_POSITION, result["status"])

        # 跌停板不能卖出
        bid_price, bid_shares, bid_time = (
            12.33,
            400,
            datetime.datetime(2022, 3, 10, 14, 55),
        )
        result = await broker.sell(tyst, bid_price, bid_shares, bid_time)
        self.assertEqual(EntrustError.REACH_SELL_LIMIT, result["status"])

        # 余额不足： 尽可能卖出
        bid_price, bid_shares, bid_time = (
            12.33,
            1100,
            datetime.datetime(2022, 3, 10, 9, 35),
        )
        result = await broker.sell(tyst, bid_price, bid_shares, bid_time)

        positions = np.array([("002537.XSHE", 2000.0, 9.02)], position_dtype)
        self._check_positions(broker.positions, positions)
        self.assertAlmostEqual(999_460.975, broker.assets, 2)
        self.assertAlmostEqual(980_100.975, broker.cash, 2)
        self.assertAlmostEqual(999_460.98 - 980_100.98, 2000 * 9.68, 2)

        # 成交量不足撮合委卖
        broker = Broker("test", 1e10, 1e-4)

        await broker.buy(tyst, 14.84, 1e8, datetime.datetime(2022, 3, 7, 9, 41))
        self._check_positions(
            broker.positions, np.array([(tyst, 802700, 14.79160334)], position_dtype)
        )

        result = await broker.sell(
            tyst, 12.33, 1e8, datetime.datetime(2022, 3, 10, 9, 35)
        )

        self.assertEqual(EntrustError.SUCCESS, result["status"])
        self.assertEqual(0, len(broker.positions))
        self.assertAlmostEqual(9_998_679_478.68, broker.assets, 2)
        self.assertAlmostEqual(broker.cash, broker.assets, 2)

    async def test_info(self):
        broker = Broker("test", 1e6, 1e-4)
        tyst, hljh = "603717.XSHG", "002537.XSHE"

        await broker.buy(tyst, 14.84, 500, datetime.datetime(2022, 3, 7, 9, 41))
        await broker.buy(tyst, 14.79, 1000, datetime.datetime(2022, 3, 8, 14, 8))
        await broker.buy(hljh, 8.95, 1000, datetime.datetime(2022, 3, 9, 9, 40))
        await broker.buy(hljh, 9.09, 1000, datetime.datetime(2022, 3, 10, 9, 33))

        await broker.sell(tyst, 12.33, 1100, datetime.datetime(2022, 3, 10, 9, 35))
        await broker.sell(hljh, 9.94, 1500, datetime.datetime(2022, 3, 14, 10, 14))

        print(broker.info)
