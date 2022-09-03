import datetime
import logging
import os
import unittest
from unittest import mock

import arrow
import cfg4py
import numpy as np
import omicron
import pytest
from omicron.models.timeframe import TimeFrame as tf
from pyemit import emit

from backtest.common.errors import EntrustError
from backtest.common.helper import get_app_context, tabulate_numpy_array
from backtest.config import get_config_dir
from backtest.feed.zillionarefeed import ZillionareFeed
from backtest.trade.broker import Broker
from backtest.trade.datatypes import (
    E_BACKTEST,
    EntrustSide,
    cash_dtype,
    daily_position_dtype,
    position_dtype,
)
from backtest.trade.trade import Trade
from tests import assert_deep_almost_equal, data_populate

logger = logging.getLogger(__name__)


class BrokerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        cfg = cfg4py.init(get_config_dir())

        try:
            os.remove("/tmp/backtest/trade.log")
            os.remove("/tmp/backtest/entrust.log")
        except FileNotFoundError:
            pass

        try:
            await omicron.init()
        except Exception:
            tf.service_degrade()

        await emit.start(emit.Engine.REDIS, start_server=True, dsn=cfg.redis.dsn)

        self.ctx = get_app_context()
        self.ctx.feed = ZillionareFeed()
        await self.ctx.feed.init()

        await data_populate()

        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        await emit.stop()
        await omicron.close()
        return await super().asyncTearDown()

    def _check_position(self, broker, actual, bid_time):
        exp = broker.get_position(bid_time)
        self.assertSetEqual(set(exp["security"]), set(actual["security"]))

        for sec in exp["security"]:
            a = exp[exp["security"] == sec]
            b = actual[actual["security"] == sec]
            self.assertAlmostEqual(a["shares"][0], b["shares"][0], 2)
            self.assertAlmostEqual(a["price"][0], b["price"][0], 2)
            self.assertAlmostEqual(a["sellable"][0], b["sellable"][0], 2)

    def _check_order_result(self, actual, sec, price, shares, commission):
        if isinstance(sec, set):
            self.assertSetEqual(set([v.security for v in actual]), sec)
        else:
            self.assertEqual(actual.security, sec)

        # exit price would be same
        if isinstance(actual, list):
            for v in actual:
                self.assertAlmostEqual(v.price, price, 2)

            sum_shares = np.sum([v.shares for v in actual])
            self.assertEqual(sum_shares, shares)
            sum_fee = np.sum([v.fee for v in actual])
            self.assertAlmostEqual(sum_fee, price * shares * commission, 2)
        else:
            self.assertAlmostEqual(actual.price, price, 2)

            self.assertEqual(actual.shares, shares)
            self.assertAlmostEqual(actual.fee, price * shares * commission, 2)

    async def test_buy(self):
        tyst = "603717.XSHG"
        hljh, principal, commission = "002537.XSHE", 1e10, 1e-4
        broker = Broker("test", principal, commission)

        async def on_backtest_event(data):
            assert isinstance(data, dict)
            logger.info("on_backtest_event: %s", data)

        emit.register(E_BACKTEST, on_backtest_event)
        # 委买部分成交
        result = await broker.buy(
            hljh,
            9.43,
            1e9,  # total available shares: 81_840_998
            datetime.datetime(2022, 3, 10, 9, 35),
        )

        price1, shares1, close_price_of_the_day = 9.324918725717664, 29265100.0, 9.68
        self._check_order_result(result, hljh, price1, shares1, commission)

        change = price1 * shares1 * (1 + commission)
        cash = broker.principal - change

        market_value = shares1 * close_price_of_the_day
        assets = cash + market_value

        positions = np.array([(hljh, shares1, 0, price1)], dtype=position_dtype)
        self._check_position(broker, positions, datetime.date(2022, 3, 10))
        self.assertAlmostEqual(assets, broker.assets, 2)
        self.assertAlmostEqual(cash, broker.cash, 2)

        # 委买当笔即全部成交
        start_cash = broker.cash  # 9727078031.93345

        result = await broker.buy(
            hljh, 9.43, 1e5, datetime.datetime(2022, 3, 10, 9, 35)
        )

        price2, shares2, close_price_of_the_day = 9.12, 1e5, 9.68

        self._check_order_result(result, hljh, price2, shares2, commission)

        shares = shares1 + shares2
        price = (price1 * shares1 + price2 * shares2) / shares
        positions = np.array([(hljh, shares, 0, price)], dtype=position_dtype)

        cash = start_cash - price2 * shares2 * (1 + commission)
        assets = cash + shares * close_price_of_the_day

        self.assertAlmostEqual(assets, broker.assets, 1)
        self.assertAlmostEqual(cash, broker.cash, 1)
        self._check_position(broker, positions, datetime.date(2022, 3, 10))

        # 买入时已经涨停
        with self.assertRaises(EntrustError) as cm:
            result = await broker.buy(
                hljh, 9.68, 10e4, datetime.datetime(2022, 3, 10, 14, 33)
            )
            self.assertEqual(cm.exception.status_code, EntrustError.REACH_BUY_LIMIT)

        # 进入到下一个交易日，此时position中应该有可以卖出的股票
        ## 买入价为11.3,全部成交
        bid_time = datetime.datetime(2022, 3, 11, 9, 35)
        price3, shares3, close_price3 = 11.1, 5e4, 11.24
        cash = broker.cash - price3 * shares3 * (1 + commission)

        ## 2022/3/11， hljh收盘价10.1
        assets = cash + shares3 * close_price3 + shares * 10.1
        positions = np.array(
            [(tyst, shares3, 0, price3), (hljh, shares, shares, price)],
            dtype=position_dtype,
        )
        await broker.buy(tyst, 11.2, 5e4, bid_time)

        self.assertAlmostEqual(assets, broker.assets, 1)
        self.assertAlmostEqual(cash, broker.cash, 1)
        self._check_position(broker, positions, bid_time.date())

        # 资金不足,委托失败
        broker._cash = np.array([(datetime.date(2022, 3, 11), 100)], dtype=cash_dtype)

        with self.assertRaises(EntrustError) as cm:
            result = await broker.buy(
                hljh, 10.20, 10e4, datetime.datetime(2022, 3, 11, 9, 35)
            )
            self.assertEqual(cm.exception.status_code, EntrustError.NO_CASH)

    async def test_get_unclosed_trades(self):
        broker = Broker("test", 1e10, 1e-4)

        self.assertEqual(0, len(broker.get_unclosed_trades(datetime.date(2022, 3, 3))))

        broker._update_unclosed_trades(0, datetime.date(2022, 3, 3))
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
            broker._update_unclosed_trades(i, dt)

        self.assertEqual(6, len(broker._unclosed_trades))
        self.assertListEqual([0], broker._unclosed_trades[datetime.date(2022, 3, 3)])
        self.assertListEqual(
            [0, 1, 2, 3], broker._unclosed_trades[datetime.date(2022, 3, 10)]
        )

    async def test_sell(self):
        broker = Broker("test", 1e6, 1e-4)
        tyst, hljh = "603717.XSHG", "002537.XSHE"

        mar_7 = datetime.datetime(2022, 3, 7, 9, 41)
        mar_8 = datetime.datetime(2022, 3, 8, 14, 8)
        mar_9 = datetime.datetime(2022, 3, 9, 9, 40)
        mar10 = datetime.datetime(2022, 3, 10, 9, 33)
        await broker.buy(tyst, 14.84, 500, mar_7)

        # 可用余额不足: 尝试卖出当天买入的部分
        bid_price, bid_shares, bid_time = (
            14.3,
            400,
            datetime.datetime(2022, 3, 7, 14, 26),
        )

        with self.assertRaises(EntrustError) as cm:
            result = await broker.sell(tyst, bid_price, bid_shares, bid_time)
            self.assertEqual(EntrustError.NO_POSITION, cm.exception.status_code)

        await broker.buy(tyst, 14.79, 1000, mar_8)
        await broker.buy(hljh, 8.95, 1000, mar_9)
        await broker.buy(hljh, 9.09, 1000, mar10)

        # 当前持仓
        ## '603717.XSHG', 1500., 1500., 14.80666667
        ## '002537.XSHE', 2000., 1000.,  9.02

        self._check_position(
            broker,
            np.array(
                [(tyst, 1500.0, 1500.0, 14.80666667), (hljh, 2000.0, 1000.0, 9.02)],
                dtype=position_dtype,
            ),
            mar10.date(),
        )

        # 可用余额足够，买单足够，close部分
        result = await broker.sell(tyst, 12.98, 1100, mar10)

        self.assertEqual(6, len(broker.trades))
        self.assertEqual(4, len(broker._unclosed_trades))
        exit_price, sold_shares = (13.67, 1100)

        self._check_order_result(  # 分两笔卖出
            result, {tyst}, exit_price, sold_shares, broker.commission
        )

        pos = np.array(
            [(tyst, 400, 400, 14.80666667), (hljh, 2000, 1000, 9.02)], position_dtype
        )
        self._check_position(broker, pos, mar10.date())
        self.assertAlmostEqual(999_073.47, broker.assets, 2)
        self.assertAlmostEqual(974_781.47, broker.cash, 2)

        # 跌停板不能卖出
        bid_price, bid_shares, bid_time = (
            12.33,
            400,
            datetime.datetime(2022, 3, 10, 14, 55),
        )

        with self.assertRaises(EntrustError) as cm:
            await broker.sell(tyst, bid_price, bid_shares, bid_time)
            self.assertEqual(EntrustError.REACH_SELL_LIMIT, cm.exception.status_code)

        # 余额不足： 尽可能卖出
        bid_price, bid_shares, bid_time = (
            11.1,
            1100,
            datetime.datetime(2022, 3, 11, 11, 30),
        )
        result = await broker.sell(tyst, bid_price, bid_shares, bid_time)

        positions = np.array(
            [("002537.XSHE", 2000.0, 1000, 9.02), ("603717.XSHG", 400, 400, 14.81)],
            position_dtype,
        )
        self._check_position(broker, positions, mar10.date())
        self.assertAlmostEqual(999_501.02, broker.assets, 2)
        self.assertAlmostEqual(979_301.02, broker.cash, 2)

        # 成交量不足撮合委卖
        broker = Broker("test", 1e10, 1e-4)

        await broker.buy(tyst, 14.84, 1e8, datetime.datetime(2022, 3, 7, 9, 41))
        self._check_position(
            broker,
            np.array([(tyst, 802700, 802700, 14.79160334)], position_dtype),
            mar10.date(),
        )

        result = await broker.sell(
            tyst, 12.33, 1e8, datetime.datetime(2022, 3, 10, 9, 35)
        )

        self.assertEqual(0, broker.position["shares"].item())
        self.assertAlmostEqual(9998678423.288, broker.assets, 2)
        self.assertAlmostEqual(broker.cash, broker.assets, 2)

        # 有除权除息的情况下，卖出
        async def make_trades():
            await broker.buy(tyst, 100, 1000, datetime.datetime(2022, 3, 8, 14, 8))
            await broker.buy(hljh, 100, 1000, datetime.datetime(2022, 3, 8, 14, 33))

            await broker.sell(tyst, 3.0, 1000, datetime.datetime(2022, 3, 9, 9, 30))
            await broker.sell(tyst, 3.0, 200, datetime.datetime(2022, 3, 10, 9, 30))
            await broker.sell(hljh, 9.1, 1200, datetime.datetime(2022, 3, 10, 9, 31))

        broker = Broker("test", 1e6, 1e-4)
        with mock.patch(
            "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor",
            side_effect=[
                # no call for 3.8
                {hljh: np.array([1, 1.2]), tyst: np.array([1, 1.2])},  # 3.9
                {hljh: np.array([1, 1]), tyst: np.array([1, 1])},  # 3.10
                {hljh: np.array([1, 1]), tyst: np.array([1, 1])},  # 3.11
                {hljh: np.array([1, 1]), tyst: np.array([1, 1])},  # 3.14
            ],
        ):
            await make_trades()
            # ensure all shares are sold out
            np.testing.assert_array_almost_equal(
                broker._positions["shares"], [1e3, 1e3, 2e2, 1.2e3, 0, 0]
            )
            np.testing.assert_array_almost_equal(
                broker._positions["sellable"], [0, 0, 0, 1e3, 0, 0]
            )

    async def test_info(self):
        # 本测试用例包含了除权除息的情况
        broker = Broker("test", 1e6, 1e-4)
        tyst, hljh = "603717.XSHG", "002537.XSHE"

        async def make_trades():
            await broker.buy(tyst, 14.84, 500, datetime.datetime(2022, 3, 7, 9, 41))
            await broker.buy(tyst, 14.79, 1000, datetime.datetime(2022, 3, 8, 14, 8))
            await broker.buy(hljh, 8.95, 1000, datetime.datetime(2022, 3, 9, 9, 40))
            await broker.buy(hljh, 9.09, 1000, datetime.datetime(2022, 3, 10, 9, 33))

            await broker.sell(tyst, 12.33, 1100, datetime.datetime(2022, 3, 10, 9, 35))
            await broker.sell(hljh, 9.94, 1500, datetime.datetime(2022, 3, 14, 10, 14))

        await make_trades()

        def assert_info_success(info):
            actual = info["positions"]
            exp = np.array(
                [
                    ("603717.XSHG", 400.0, 400.0, 14.80666667),
                    ("002537.XSHE", 500.0, 500.0, 9.02),
                ],
                dtype=position_dtype,
            )
            np.testing.assert_array_equal(actual["security"], exp["security"])
            np.testing.assert_array_equal(actual["shares"], exp["shares"])
            np.testing.assert_array_equal(actual["sellable"], exp["sellable"])
            np.testing.assert_array_almost_equal(actual["price"], exp["price"], 2)

            self.assertEqual(datetime.date(2022, 3, 7), info["start"])
            self.assertEqual(datetime.date(2022, 3, 14), info["last_trade"])
            self.assertAlmostEqual(998407.99, info["assets"], 2)
            self.assertAlmostEqual(989579.99, info["available"], 2)
            self.assertAlmostEqual(8828.0, info["market_value"], 2)
            self.assertAlmostEqual(info["assets"] - info["principal"], info["pnl"], 2)

        with mock.patch(
            "arrow.now", return_value=datetime.datetime(2022, 3, 14, 9, 31)
        ):
            info1 = await broker.info()
            assert_info_success(info1)

        cash1 = broker.get_cash(datetime.date(2022, 3, 9))
        assets1 = await broker.get_assets(datetime.date(2022, 3, 9))

        # assume xdxr happend at 2022/3/9 on tyst
        broker = Broker("test", 1e6, 1e-4)

        logger.info("check info with xdxr")
        with mock.patch(
            "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor",
            side_effect=[
                # no call for 3.7
                {hljh: np.array([1, 1]), tyst: np.array([1, 1])},
                {hljh: np.array([1, 1]), tyst: np.array([1, 1])},
                {hljh: np.array([1, 1.2]), tyst: np.array([1, 1.2])},  # 3.10
                {hljh: np.array([1, 1.0, 1.2]), tyst: np.array([1, 1.0, 1.2])},
            ],
        ):
            await make_trades()

        with mock.patch(
            "arrow.now", return_value=datetime.datetime(2022, 3, 14, 9, 31)
        ):
            exp = np.array(
                [
                    (datetime.date(2022, 3, 9), "002537.XSHE", 1000.0, 0.0, 8.95),
                    (datetime.date(2022, 3, 10), "002537.XSHE", 2200.0, 1000.0, 8.2),
                    (datetime.date(2022, 3, 11), "002537.XSHE", 2200.0, 2200.0, 8.2),
                    (datetime.date(2022, 3, 14), "002537.XSHE", 1140, 700.0, 6.83),
                ],
                dtype=daily_position_dtype,
            )

            hljh_arr = broker._positions[broker._positions["security"] == hljh]
            for key in ("shares", "sellable", "price"):
                np.testing.assert_array_almost_equal(exp[key], hljh_arr[key], 2)

            np.testing.assert_array_equal(exp["date"], hljh_arr["date"])

            exp = np.array(
                [
                    (datetime.date(2022, 3, 7), "603717.XSHG", 500.0, 0.0, 14.84),
                    (
                        datetime.date(2022, 3, 8),
                        "603717.XSHG",
                        1500.0,
                        500.0,
                        14.80666667,
                    ),
                    (
                        datetime.date(2022, 3, 9),
                        "603717.XSHG",
                        1500.0,
                        1500.0,
                        14.80666667,
                    ),
                    (
                        datetime.date(2022, 3, 10),
                        "603717.XSHG",
                        700.0,
                        400.0,
                        12.33888889,
                    ),
                    (
                        datetime.date(2022, 3, 11),
                        "603717.XSHG",
                        700.0,
                        700.0,
                        12.33888889,
                    ),
                    (
                        datetime.date(2022, 3, 14),
                        "603717.XSHG",
                        840.0,
                        700.0,
                        10.28240741,
                    ),
                ],
                dtype=daily_position_dtype,
            )

            tyst_arr = broker._positions[broker._positions["security"] == tyst]
            for key in ("shares", "sellable", "price"):
                np.testing.assert_array_almost_equal(exp[key], tyst_arr[key], 2)
            np.testing.assert_array_equal(exp["date"], tyst_arr["date"])

            info = await broker.info()
            self.assertAlmostEqual(1008979.19, info["assets"], 2)
            self.assertAlmostEqual(989579.99, info["available"], 2)

            assets = await broker.get_assets(datetime.date(2022, 3, 10))
            cash = broker.get_cash(datetime.date(2022, 3, 10))
            self.assertAlmostEqual(974671.48, cash, 2)
            self.assertAlmostEqual(1004598.48, assets, 2)

        # test get info at special date
        broker = Broker("test", 1e6, 1e-4)
        await make_trades()
        info2 = await broker.info(datetime.date(2022, 3, 14))
        assert_info_success(info2)

        info3 = await broker.info(datetime.date(2022, 3, 9))
        self.assertAlmostEqual(998186.88, info3["assets"], 2)
        self.assertAlmostEqual(968836.88, info3["available"], 2)
        self.assertAlmostEqual(29350.0, info3["market_value"], 2)

    def test_str_repr(self):
        broker = Broker("test", 1e6, 1e-4)
        exp = "\n".join(
            [
                "账户：test:",
                "    总资产：1,000,000.00",
                "    本金：1,000,000.00",
                "    可用资金：1,000,000.00",
                "    持仓：[]\n",
            ]
        )
        self.assertEqual(exp, str(broker))

    @mock.patch("arrow.now", return_value=arrow.get("2022-03-14 15:00:00"))
    async def test_metrics(self, mock_now):
        broker = Broker("test", 1e6, 1e-4)
        hljh = "002537.XSHE"

        await broker.buy(hljh, 9.13, 500, datetime.datetime(2022, 3, 1, 9, 31))
        await broker.buy(hljh, 10.03, 500, datetime.datetime(2022, 3, 2, 9, 31))
        await broker.buy(hljh, 11.05, 500, datetime.datetime(2022, 3, 3, 9, 31))
        await broker.buy(hljh, 10.47, 500, datetime.datetime(2022, 3, 4, 9, 31))
        await broker.buy(hljh, 9.41, 500, datetime.datetime(2022, 3, 7, 9, 31))
        await broker.buy(hljh, 9.57, 500, datetime.datetime(2022, 3, 8, 9, 31))
        await broker.buy(hljh, 9.08, 500, datetime.datetime(2022, 3, 9, 9, 31))
        await broker.buy(hljh, 9.1, 500, datetime.datetime(2022, 3, 10, 9, 31))
        await broker.buy(hljh, 9.68, 500, datetime.datetime(2022, 3, 11, 9, 31))
        await broker.buy(hljh, 9.65, 500, datetime.datetime(2022, 3, 14, 9, 31))
        await broker.sell(hljh, 9.1, 5000, datetime.datetime(2022, 3, 14, 15))

        actual = await broker.metrics(baseline=hljh)
        exp = {
            "start": datetime.date(2022, 3, 1),
            "end": datetime.date(2022, 3, 14),
            "window": 10,
            "total_tx": 9,
            "total_profit": -404.0999999998603,
            "total_profit_rate": -0.0004040999999998603,
            "win_rate": 0.5555555555555556,
            "mean_return": -3.896698729980441e-05,
            "sharpe": -1.396890251070207,
            "sortino": -2.0486727998320817,
            "calmar": -2.422741706100782,
            "max_drawdown": -0.0041827334569883405,
            "annual_return": -0.010133682791748755,
            "volatility": 0.02850594795764624,
            "baseline": {
                "code": "002537.XSHE",
                "win_rate": 0.5555555555555556,
                "sharpe": 0.6190437353475076,
                "max_drawdown": -0.17059373779725692,
                "sortino": 1.0015572769806516,
                "annual_return": 0.19278435493450163,
                "total_profit_rate": 0.006315946578979492,
                "volatility": 1.1038380776228978,
            },
        }

        assert_deep_almost_equal(self, actual, exp, places=4)

    async def test_assets(self):
        broker = Broker("test", 1e6, 1e-4)
        hljh = "002537.XSHE"

        await broker.buy(hljh, 10, 500, datetime.datetime(2022, 3, 1, 9, 31))

        # 持仓股有停牌, https://github.com/zillionare/backtesting/issues/14
        with mock.patch(
            "omicron.models.stock.Stock.get_bars",
            side_effect=[np.array([]), np.array([(10.0,)], dtype=[("close", "f4")])],
        ):
            assets = await broker.get_assets(datetime.date(2022, 3, 7))
            self.assertAlmostEqual(assets, broker.cash + 10 * 500, 2)

        broker = Broker("test", 1e6, 1e-4)
        await broker.buy(hljh, 12, 500, datetime.datetime(2022, 3, 4, 9, 31))
        with mock.patch(
            "omicron.models.stock.Stock.get_bars",
            side_effect=[np.array([]), np.array([])],
        ):
            assets = await broker.get_assets(datetime.date(2022, 3, 7))
            price = broker._positions[-1]["price"]
            self.assertAlmostEqual(assets, broker.cash + 500 * price, 2)

        broker = Broker("test", 1e6, 1e-4)
        await broker.buy(hljh, 12, 500, datetime.datetime(2022, 3, 1, 9, 31))
        with mock.patch(
            "omicron.models.stock.Stock.get_bars",
            side_effect=[
                np.array([(i,)], dtype=[("close", "f4")]) for i in range(1, 10)
            ],
        ):
            for i in range(1, 10):
                dt = tf.day_shift(datetime.date(2022, 3, 1), i)
                actual = await broker.get_assets(dt)
                self.assertAlmostEqual(actual, broker.cash + 500 * i)

    async def test_before_trade(self):
        """this also test get_cash"""
        broker = Broker("test", 1e6, 1e-4)
        hljh = "002537.XSHE"
        tyst = "603717.XSHG"

        await broker.buy(hljh, 9.13, 500, datetime.datetime(2022, 3, 1, 9, 31))
        await broker.buy(hljh, 10.03, 500, datetime.datetime(2022, 3, 4, 9, 31))
        await broker.buy(tyst, 14.84, 1500, datetime.datetime(2022, 3, 7, 9, 31))

        self.assertEqual(6, broker._assets.size)
        self.assertEqual(datetime.date(2022, 2, 28), broker._assets[0]["date"])
        self.assertEqual(datetime.date(2022, 3, 7), broker._assets[-1]["date"])

        self.assertListEqual(
            [
                datetime.date(2022, 2, 28),
                datetime.date(2022, 3, 1),
                datetime.date(2022, 3, 2),
                datetime.date(2022, 3, 3),
                datetime.date(2022, 3, 4),
                datetime.date(2022, 3, 7),
            ],
            broker._cash["date"].tolist(),
        )

        self.assertListEqual(
            [
                datetime.date(2022, 3, 1),
                datetime.date(2022, 3, 2),
                datetime.date(2022, 3, 3),
                datetime.date(2022, 3, 4),
                datetime.date(2022, 3, 7),
                datetime.date(2022, 3, 7),
            ],
            broker._positions["date"].tolist(),
        )

        self.assertListEqual(
            [500, 500, 500, 1000, 1000, 1500], broker._positions["shares"].tolist()
        )

        self.assertListEqual(
            [0, 500, 500, 500, 1000, 0], broker._positions["sellable"].tolist()
        )

        # check when there's xdxr
        with mock.patch("arrow.now", return_value=datetime.datetime(2022, 3, 14, 15)):
            with mock.patch(
                "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor",
                side_effect=[
                    {hljh: np.array([1, 1, 1, 1])},
                    {hljh: np.array([1, 1.2])},
                    {hljh: np.array([1, 1, 1, 1, 1, 1])},
                ],
            ):
                broker = Broker("test", 1e6, 1e-4)
                hljh = "002537.XSHE"

                await broker.buy(hljh, 9.13, 500, datetime.datetime(2022, 3, 1, 9, 31))
                await broker.buy(hljh, 10.03, 500, datetime.datetime(2022, 3, 4, 9, 31))
                await broker.sell(hljh, 9, 500, datetime.datetime(2022, 3, 7, 9, 31))

                self.assertListEqual(
                    broker._positions["shares"].tolist(), [500, 500, 500, 1000, 700]
                )
                self.assertListEqual(
                    broker._positions["sellable"].tolist(), [0, 500, 500, 500, 500]
                )

    async def test_get_position(self):
        broker = Broker("test", 1e6, 1e-4)
        hljh = "002537.XSHE"

        self.assertEqual(0, broker.position.size)

        await broker.buy(hljh, 9.13, 500, datetime.datetime(2022, 3, 1, 9, 31))
        self.assertEqual(0, broker.position["sellable"].item())

        # 查询过往持仓
        sellable = broker.get_position(datetime.date(2022, 3, 2))["sellable"].item()
        self.assertEqual(500, sellable)

        # next day, it's all sellable
        sellable = broker.get_position(datetime.date(2022, 3, 4))["sellable"].item()
        self.assertEqual(500, sellable)

        await broker.sell(hljh, 9.59, 500, datetime.datetime(2022, 3, 4, 9, 31))
        self.assertEqual(0, broker.position["shares"].item())

        # 查询过往持仓
        sellable = broker.get_position(datetime.date(2022, 3, 2))["sellable"].item()
        self.assertEqual(500, sellable)

    @mock.patch("arrow.now", return_value=arrow.get("2022-03-14 15:00:00"))
    async def test_recalc_assets(self, mocked_now):
        # 回测模式
        bt_start = datetime.date(2022, 3, 1)
        bt_stop = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, bt_start, bt_stop)
        hljh = "002537.XSHE"
        tyst = "603717.XSHG"

        await broker.recalc_assets()
        self.assertEqual(bt_start, broker._assets[1]["date"])
        self.assertEqual(bt_stop, broker._assets[-1]["date"])
        self.assertEqual(11, broker._assets.size)

        self.assertListEqual([1e6] * 11, broker._assets["assets"].tolist())

        broker = Broker("test", 1e6, 1e-4, bt_start, bt_stop)
        await broker.buy(hljh, 10.03, 500, datetime.datetime(2022, 3, 4, 9, 31))
        await broker.buy(tyst, 14.84, 1500, datetime.datetime(2022, 3, 7, 9, 31))

        # should have assets up to 2022, 3, 7
        self.assertEqual(6, broker._assets.size)
        self.assertEqual(datetime.date(2022, 3, 7), broker._assets[-1]["date"])
        await broker.recalc_assets()

        exp = np.array(
            [
                (datetime.date(2022, 2, 28), 1000000.0),
                (datetime.date(2022, 3, 1), 1000000.0),
                (datetime.date(2022, 3, 2), 1000000.0),
                (datetime.date(2022, 3, 3), 1000000.0),
                (datetime.date(2022, 3, 4), 999864.50717168),
                (datetime.date(2022, 3, 7), 1000022.28504219),
                (datetime.date(2022, 3, 8), 999507.28504219),
                (datetime.date(2022, 3, 9), 997802.28504219),
                (datetime.date(2022, 3, 10), 996187.28504219),
                (datetime.date(2022, 3, 11), 994762.28504219),
                (datetime.date(2022, 3, 14), 992812.28504219),
            ],
            dtype=[("date", "O"), ("assets", "<f8")],
        )

        np.testing.assert_array_equal(exp["date"], broker._assets["date"])
        np.testing.assert_array_almost_equal(
            exp["assets"], broker._assets["assets"], decimal=2
        )

        with mock.patch("arrow.now", return_value=arrow.get("2022-03-10 09:31:00")):
            broker = Broker("test", 1e6, 1e-4)

            await broker.recalc_assets()
            self.assertEqual(0, broker._assets.size)

            await broker.buy(hljh, 10.03, 500, datetime.datetime(2022, 3, 4, 9, 31))
            await broker.buy(tyst, 14.84, 1500, datetime.datetime(2022, 3, 7, 9, 31))

            await broker.recalc_assets()

            exp = np.array(
                [
                    (datetime.date(2022, 3, 3), 1e6),
                    (datetime.date(2022, 3, 4), 999864.50717168),
                    (datetime.date(2022, 3, 7), 1000022.28504219),
                    (datetime.date(2022, 3, 8), 999507.28504219),
                    (datetime.date(2022, 3, 9), 997802.28504219),
                    (datetime.date(2022, 3, 10), 996187.28504219),
                ],
                dtype=[("date", "O"), ("assets", "<f8")],
            )

            np.testing.assert_array_equal(exp["date"], broker._assets["date"])
            np.testing.assert_array_almost_equal(
                exp["assets"], broker._assets["assets"], decimal=2
            )

        # 遇到停牌的情况，一个使用停牌前价格，一个使用成交均价
        broker = Broker("test", 1e6, 1e-4)

        await broker.buy(hljh, 10.03, 500, datetime.datetime(2022, 3, 4, 9, 31))
        await broker.buy(tyst, 14.84, 1500, datetime.datetime(2022, 3, 4, 9, 31))

        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                tyst: np.array(
                    [],
                    dtype=[("frame", "O"), ("close", "<f4")],
                ),
                hljh: np.array(
                    [
                        (datetime.datetime(2022, 3, 4), 9.59),
                        (datetime.datetime(2022, 3, 7), 9.67),
                        (datetime.datetime(2022, 3, 8), 9.12),
                        (datetime.datetime(2022, 3, 9), 8.8),
                        (datetime.datetime(2022, 3, 10), 9.68),
                        (datetime.datetime(2022, 3, 11), 10.1),
                        (datetime.datetime(2022, 3, 14), 9.56),
                    ],
                    dtype=[("frame", "O"), ("close", "<f4")],
                ),
            }.items()

            await broker.recalc_assets()
            exp = np.array(
                [
                    (datetime.date(2022, 3, 3), 1e6),
                    (datetime.date(2022, 3, 4), 999877.29),
                    (datetime.date(2022, 3, 7), 992837.29),
                    (datetime.date(2022, 3, 8), 992562.29),
                    (datetime.date(2022, 3, 9), 992402.29),
                    (datetime.date(2022, 3, 10), 992842.29),
                    (datetime.date(2022, 3, 11), 993052.29),
                    (datetime.date(2022, 3, 14), 992782.29),
                ],
                dtype=[("date", "O"), ("assets", "<f8")],
            )

            np.testing.assert_array_equal(exp["date"], broker._assets["date"])
            np.testing.assert_array_almost_equal(
                exp["assets"], broker._assets["assets"], decimal=2
            )

        # issue 23,
        broker = Broker("test", 1e6, 1e-4)
        await broker.buy(hljh, 10.03, 500, datetime.datetime(2022, 3, 4, 9, 31))
        await broker.sell(hljh, 0, 500, datetime.datetime(2022, 3, 7, 9, 31))
        await broker.buy(hljh, 12, 5e4, datetime.datetime(2022, 3, 10, 9, 31))

        await broker.recalc_assets()
        exp = [
            1e6,
            999864.51,
            999744.04,
            999744.04,
            999744.04,
            1026198.32125,
            1047198.34,
            1020198.34,
        ]
        np.testing.assert_array_almost_equal(exp, broker._assets["assets"], decimal=2)

    async def test_update_positions(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1_000_000, 1e-4, start, end)

        bid_time = datetime.datetime(2022, 3, 1, 9, 31)
        trade = Trade(
            "01", "002537.XSHE", 10, 500, 500 * 1e-4 * 10, EntrustSide.BUY, bid_time
        )
        await broker._update_positions(trade, bid_time.date())
        print(tabulate_numpy_array(broker._positions))

        # add buy trade later
        bid_time = datetime.datetime(2022, 3, 4, 9, 31)
        trade = Trade(
            "02", "002537.XSHE", 20, 1000, 1000 * 20 * 1e-4, EntrustSide.BUY, bid_time
        )

        await broker._update_positions(trade, bid_time.date())

    async def test_calendar_validation(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1_000_000, 1e-4, start, end)

        await broker.buy(
            "002537.XSHE", 10.03, 500, datetime.datetime(2022, 3, 4, 9, 32)
        )

        with self.assertRaises(EntrustError) as cm:
            await broker.buy(
                "002537.XSHE", 14.84, 1500, datetime.datetime(2022, 3, 4, 9, 31)
            )
        self.assertEqual(cm.exception.status_code, EntrustError.TIME_REWIND)

    async def test_fillup_positions(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1_000_000, 1e-4, start, end)

        hljh = "002537.XSHE"
        tyst = "603717.XSHG"
        await broker.buy(hljh, None, 500, datetime.datetime(2022, 3, 1, 9, 30))
        await broker.buy(tyst, None, 1500, datetime.datetime(2022, 3, 1, 9, 30))

        with mock.patch("arrow.now", return_value=datetime.datetime(2022, 3, 14, 15)):
            with mock.patch(
                "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor",
                return_value={hljh: np.array([1.0] * 5), tyst: np.array([1.0] * 5)},
            ):
                await broker._fillup_positions(datetime.datetime(2022, 3, 7, 9, 31))
                exp = [500, 1500] * 5
                np.testing.assert_almost_equal(broker._positions["shares"], exp)

        with mock.patch(
            "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor",
            return_value={
                hljh: np.array([1.0, 1.1, 1.1, 1.1, 1.1, 1.2]),
                tyst: np.array([1.0, 1.0, 1.2, 1.2, 1.2, 1.3]),
            },
        ):  # fillup from 3.7 to 3.14, 5 days in total
            await broker._fillup_positions(datetime.datetime(2022, 3, 14, 9, 31))

            np.testing.assert_array_almost_equal(
                [550, 550, 550, 550, 600],
                broker._positions[broker._positions["security"] == hljh]["shares"][-5:],
                decimal=2,
            )

            np.testing.assert_almost_equal(
                [8.43, 8.43, 8.43, 8.43, 7.73],
                broker._positions[broker._positions["security"] == hljh]["price"][-5:],
                decimal=2,
            )

            np.testing.assert_array_almost_equal(
                [1500, 1800, 1800, 1800, 1950],
                broker._positions[broker._positions["security"] == tyst]["shares"][-5:],
                decimal=2,
            )
            np.testing.assert_array_almost_equal(
                [15.45, 12.88, 12.88, 12.88, 11.88],
                broker._positions[broker._positions["security"] == tyst]["price"][-5:],
                decimal=2,
            )

        # issue 9, 对持仓为0的股，不查询价格和dr信息
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test_fillup_positions", 1_000_000, 1e-4, start, end)

        hljh = "002537.XSHE"
        tyst = "603717.XSHG"
        await broker.buy(hljh, None, 500, datetime.datetime(2022, 3, 1, 9, 30))
        await broker.buy(tyst, None, 1500, datetime.datetime(2022, 3, 1, 9, 30))
        await broker.sell(hljh, None, 500, datetime.datetime(2022, 3, 4, 9, 31))
        await broker.sell(tyst, None, 1500, datetime.datetime(2022, 3, 9, 9, 40))
        await broker.buy(tyst, None, 1500, datetime.datetime(2022, 3, 10, 9, 41))

        with mock.patch("arrow.now", return_value=datetime.datetime(2022, 3, 14, 15)):
            await broker.buy(hljh, None, 100, datetime.datetime(2022, 3, 14, 9, 31))
            pass

        # issue 17,当发生除权时，新生成的trade的价格被错误地置为数组，而不是标量
        broker = Broker("test", 1e6, 1e-4)

        logger.info("check info with xdxr")
        broker = Broker("test", 1e6, 1e-4)
        tyst, hljh = "603717.XSHG", "002537.XSHE"

        async def make_trades():
            await broker.buy(tyst, 14.84, 500, datetime.datetime(2022, 3, 7, 9, 41))
            await broker.sell(tyst, 1.0, 1100, datetime.datetime(2022, 3, 14, 9, 35))

        with mock.patch(
            "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor",
            side_effect=[
                # no call for 3.7
                {tyst: np.array([1, 1.1, 1.1, 1.2, 1.2, 1.4])},
            ],
        ):
            await make_trades()

        total_shares = 500 * 0.4
        dr_shares = 0
        actual = {}
        for trade in broker.trades.values():
            if trade.side == EntrustSide.XDXR:
                self.assertEqual(type(trade.price), float)
                actual[trade.time.date()] = trade.price
                dr_shares += trade.shares

        self.assertEqual(dr_shares, total_shares)
        self.assertListEqual(
            [
                datetime.date(2022, 3, 8),
                datetime.date(2022, 3, 10),
                datetime.date(2022, 3, 14),
            ],
            list(actual.keys()),
        )
        np.testing.assert_array_almost_equal(
            [13.49, 12.37, 10.6], list(actual.values()), decimal=2
        )
