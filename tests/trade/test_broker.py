import datetime
import os
import pickle
import unittest
from typing import Union
from unittest import mock

import arrow
import cfg4py
import numpy as np
import omicron
import pandas as pd
import pytest
from coretypes import FrameType
from coretypes.errors.trade import (
    AccountStoppedError,
    BadParamsError,
    BuylimitError,
    CashError,
    PositionError,
    SellLimitError,
    TimeRewindError,
)
from omicron.core.backtestlog import BacktestLogger
from omicron.models.timeframe import TimeFrame as tf
from pyemit import emit

from backtest.common.helper import get_app_context, tabulate_numpy_array
from backtest.config import get_config_dir
from backtest.feed import match_data_dtype
from backtest.feed.zillionarefeed import ZillionareFeed
from backtest.trade.broker import Broker
from backtest.trade.datatypes import (
    E_BACKTEST,
    EntrustSide,
    assets_dtype,
    cash_dtype,
    daily_position_dtype,
    position_dtype,
)
from backtest.trade.trade import Trade
from tests import assert_deep_almost_equal, bars_from_csv, data_populate

hljh = "002537.XSHE"
tyst = "603717.XSHG"

feb28 = datetime.date(2022, 2, 28)
mar1 = datetime.date(2022, 3, 1)
mar2 = datetime.date(2022, 3, 2)
mar3 = datetime.date(2022, 3, 3)
mar4 = datetime.date(2022, 3, 4)
mar7 = datetime.date(2022, 3, 7)
mar8 = datetime.date(2022, 3, 8)
mar9 = datetime.date(2022, 3, 9)
mar10 = datetime.date(2022, 3, 10)
mar11 = datetime.date(2022, 3, 11)
mar14 = datetime.date(2022, 3, 14)

logger = BacktestLogger.getLogger(__name__)


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

    async def test_get_cash(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 4, 1)
        principal = 1_000_000
        broker = Broker("test", principal, 1e-4, start, end)
        self.assertAlmostEqual(principal, broker.get_cash(start), 2)
        self.assertAlmostEqual(principal, broker.get_cash(end), 2)

        broker._cash = np.array(
            [
                (mar1, 1e6),
                (mar2, 2e6),
                (mar3, 3e6),
                (mar4, 4e6),
                (mar7, 5e6),
                (mar8, 6e6),
                (mar9, 7e6),
            ],
            dtype=cash_dtype,
        )

        self.assertEqual(broker.get_cash(mar4), 4e6)
        self.assertEqual(broker.get_cash(datetime.date(2022, 3, 6)), 4e6)

        broker._forward_cashtable(end)
        self.assertAlmostEqual(7e6, broker.get_cash(end), 2)

    async def test_buy(self):
        tyst = "603717.XSHG"
        hljh, principal, commission = "002537.XSHE", 1e10, 1e-4
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", principal, commission, start, end)

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
        await broker._forward_assets(mar10)
        self.assertAlmostEqual(assets, broker.assets, 0)
        self.assertAlmostEqual(cash, broker.cash, 0)

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

        await broker._forward_assets(mar10)
        self.assertAlmostEqual(assets, broker.assets, 1)
        self.assertAlmostEqual(cash, broker.cash, 1)
        self._check_position(broker, positions, datetime.date(2022, 3, 10))

        # 买入时已经涨停
        with self.assertRaises(BuylimitError) as cm:
            result = await broker.buy(
                hljh, 9.68, 1.0e5, datetime.datetime(2022, 3, 10, 14, 33)
            )
            self.assertTrue(isinstance(cm, BuylimitError))

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
        await broker._forward_assets(mar11)
        self.assertAlmostEqual(assets, broker.assets, 1)
        self.assertAlmostEqual(cash, broker.cash, 1)
        self._check_position(broker, positions, bid_time.date())

        # 资金不足,委托失败
        broker._cash = np.array([(datetime.date(2022, 3, 11), 100)], dtype=cash_dtype)

        with self.assertRaises(CashError) as cm:
            result = await broker.buy(
                hljh, 10.20, 10e4, datetime.datetime(2022, 3, 11, 9, 35)
            )
            self.assertTrue(isinstance(cm, CashError))

        # 当出现跌停时，可以无限买入 (hljh 3.4日)
        broker = Broker("test", principal, commission, mar1, mar14)

        # 有跌停，全部成交
        result = await broker.buy(
            hljh,
            10,
            2e10,  # total available shares: 81_840_998
            datetime.datetime(2022, 3, 4, 9, 30),
        )
        self.assertEqual(999900000, result.shares)

    async def test_get_unclosed_trades(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e10, 1e-4, start, end)

        self.assertEqual(0, len(broker.get_unclosed_trades(datetime.date(2022, 3, 3))))

        broker._update_unclosed_trades(0, datetime.date(2022, 3, 3))
        self.assertListEqual([0], broker.get_unclosed_trades(datetime.date(2022, 3, 3)))

        self.assertListEqual([0], broker.get_unclosed_trades(datetime.date(2022, 3, 4)))
        self.assertEqual(2, len(broker._unclosed_trades))

    async def test_append_unclosed_trades(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e10, 1e-4, start, end)

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
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, start, end)
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

        with self.assertRaises(PositionError) as cm:
            result = await broker.sell(tyst, bid_price, bid_shares, bid_time)
            self.assertTrue(isinstance(cm, PositionError))

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
        await broker._forward_assets(mar10.date())
        self.assertAlmostEqual(999_073.48, broker.assets, 2)
        self.assertAlmostEqual(974_781.48, broker.cash, 2)

        # 跌停板不能卖出
        bid_price, bid_shares, bid_time = (
            12.33,
            400,
            datetime.datetime(2022, 3, 10, 14, 55),
        )

        with self.assertRaises(SellLimitError) as cm:
            await broker.sell(tyst, bid_price, bid_shares, bid_time)
            self.assertTrue(isinstance(cm, SellLimitError))

        # 余额不足： 尽可能卖出
        bid_price, bid_shares, bid_time = (
            11.1,
            1100,
            datetime.datetime(2022, 3, 11, 11, 30),
        )
        result = await broker.sell(tyst, bid_price, bid_shares, bid_time)

        positions_10 = np.array(
            [("002537.XSHE", 2000.0, 1000, 9.02), ("603717.XSHG", 400, 400, 14.81)],
            position_dtype,
        )
        positions_11 = np.array(
            [("002537.XSHE", 2000.0, 2000, 9.02), ("603717.XSHG", 0, 0, 0)],
            position_dtype,
        )
        self._check_position(broker, positions_10, mar10.date())
        self._check_position(broker, positions_11, mar11)
        await broker._forward_assets(mar11)
        self.assertAlmostEqual(999_501.03, broker.assets, 2)
        self.assertAlmostEqual(979_301.03, broker.cash, 2)

        # 成交量不足撮合委卖
        broker = Broker("test", 1e10, 1e-4, start, end)

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
        await broker._forward_assets(mar10.date())
        self.assertAlmostEqual(9998678423.08, broker.assets, 2)
        self.assertAlmostEqual(broker.cash, broker.assets, 2)

        # 有除权除息的情况下，卖出
        async def make_trades():
            await broker.buy(tyst, 100, 1000, datetime.datetime(2022, 3, 8, 14, 8))
            await broker.buy(hljh, 100, 1000, datetime.datetime(2022, 3, 8, 14, 33))

            await broker.sell(tyst, 3.0, 1000, datetime.datetime(2022, 3, 9, 9, 30))
            await broker.sell(tyst, 3.0, 200, datetime.datetime(2022, 3, 10, 9, 30))
            await broker.sell(hljh, 9.1, 1200, datetime.datetime(2022, 3, 10, 9, 31))

        broker = Broker("test", 1e6, 1e-4, start, end)
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
            filter = broker._positions["date"] >= mar8
            # ensure all shares are sold out
            np.testing.assert_array_almost_equal(
                broker._positions[filter]["shares"], [1e3, 1e3, 2e2, 1.2e3, 0, 0]
            )
            np.testing.assert_array_almost_equal(
                broker._positions[filter]["sellable"], [0, 0, 0, 1e3, 0, 0]
            )

        # 有涨停的情况下，卖出全部成交（hljh, 3月2日）
        broker = Broker("test", 1e6, 1e-4, mar1, mar14)

    async def test_info(self):
        # 本测试用例包含了除权除息的情况
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, start, end)
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

            self.assertEqual(datetime.date(2022, 3, 1), info["start"])
            self.assertEqual(datetime.date(2022, 3, 14), info["last_trade"])
            self.assertAlmostEqual(998407.999, info["assets"], 2)
            self.assertAlmostEqual(989579.999, info["available"], 2)
            self.assertAlmostEqual(8828.0, info["market_value"], 2)
            self.assertAlmostEqual(info["assets"] - info["principal"], info["pnl"], 2)

        # 1. 获取info直到最后交易日，也就是mar14
        with mock.patch(
            "arrow.now", return_value=datetime.datetime(2022, 3, 14, 9, 31)
        ):
            info1 = await broker.info()
            assert_info_success(info1)

        # 2. 重复测试，这一次分别在10, 14号引入了除权
        broker = Broker("test", 1e6, 1e-4, start, end)

        logger.info("check info with xdxr")
        with mock.patch(
            "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor",
            side_effect=[
                pd.DataFrame(
                    {tyst: [1, 1]}, index=[datetime.date(2022, 3, i) for i in (7, 8)]
                ),
                pd.DataFrame(
                    {tyst: [1, 1]}, index=[datetime.date(2022, 3, i) for i in (8, 9)]
                ),
                pd.DataFrame(
                    {hljh: [1, 1.2], tyst: [1, 1.2]},
                    index=[datetime.date(2022, 3, i) for i in (9, 10)],
                ),
                pd.DataFrame(
                    {hljh: [1, 1, 1.2], tyst: [1, 1, 1.2]},
                    index=[datetime.date(2022, 3, i) for i in (10, 11, 14)],
                ),
            ],
        ):
            await make_trades()

        with mock.patch(
            "arrow.now", return_value=datetime.datetime(2022, 3, 14, 9, 31)
        ):
            exp = np.array(
                [
                    (mar9, hljh, 1000.0, 0.0, 8.95),
                    (mar10, hljh, 2200.0, 1000.0, 8.2),
                    (mar11, hljh, 2200.0, 2200.0, 8.2),
                    (mar14, hljh, 1140, 700.0, 6.83),
                ],
                dtype=daily_position_dtype,
            )

            hljh_arr = broker._positions[broker._positions["security"] == hljh]
            for key in ("shares", "sellable", "price"):
                np.testing.assert_array_almost_equal(exp[key], hljh_arr[key], 2)

            np.testing.assert_array_equal(exp["date"], hljh_arr["date"])

            exp = np.array(
                [
                    (mar7, tyst, 500.0, 0.0, 14.84),
                    (mar8, tyst, 1500.0, 500.0, 14.80666667),
                    (mar9, tyst, 1500.0, 1500.0, 14.80666667),
                    (mar10, tyst, 700.0, 400.0, 12.33888889),
                    (mar11, tyst, 700.0, 700.0, 12.33888889),
                    (mar14, tyst, 840.0, 700.0, 10.28240741),
                ],
                dtype=daily_position_dtype,
            )

            tyst_arr = broker._positions[broker._positions["security"] == tyst]
            for key in ("shares", "sellable", "price"):
                np.testing.assert_array_almost_equal(exp[key], tyst_arr[key], 2)
            np.testing.assert_array_equal(exp["date"], tyst_arr["date"])

            info = await broker.info()
            self.assertAlmostEqual(1008979.2, info["assets"], 2)
            self.assertAlmostEqual(989580, info["available"], 2)

            assets = await broker.get_assets(datetime.date(2022, 3, 10))
            cash = broker.get_cash(datetime.date(2022, 3, 10))
            self.assertAlmostEqual(974671.49, cash, 2)
            self.assertAlmostEqual(1004598.49, assets, 2)

        # 3. 获取某个特定日期的info
        broker = Broker("test", 1e6, 1e-4, mar1, mar14)
        await make_trades()
        info2 = await broker.info(datetime.date(2022, 3, 14))
        assert_info_success(info2)

        info3 = await broker.info(datetime.date(2022, 3, 9))
        self.assertAlmostEqual(998186.89, info3["assets"], 2)
        self.assertAlmostEqual(968836.89, info3["available"], 2)
        self.assertAlmostEqual(29350.0, info3["market_value"], 2)

    def test_str_repr(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, start, end)
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
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, start, end)
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

        await broker.stop_backtest()
        actual = await broker.metrics(baseline=hljh)
        exp = {
            "start": datetime.date(2022, 3, 1),
            "end": datetime.date(2022, 3, 14),
            "window": 10,
            "total_tx": 9,
            "total_profit": -404.0988,
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
                "total_profit_rate": 0.006315946578979492,
                "win_rate": 0.5555555555555556,
                "mean_return": 0.0028306511,
                "sharpe": 0.6190437353475076,
                "sortino": 1.0015572769806516,
                "calmar": 1.1300787322194514,
                "max_drawdown": -0.17059373779725692,
                "annual_return": 0.19278435493450163,
                "volatility": 1.1038380776228978,
            },
        }

        assert_deep_almost_equal(self, actual, exp, places=4)

    async def test_get_assets(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, start, end)
        hljh = "002537.XSHE"

        await broker.buy(hljh, 10, 500, datetime.datetime(2022, 3, 1, 9, 31))

        # 持仓股有停牌, https://github.com/zillionare/backtesting/issues/14
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range",
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                hljh: np.array(
                    [
                        (mar7, 10),
                    ],
                    dtype=[("frame", "datetime64[s]"), ("close", "<f4")],
                ),
            }.items()

            assets = await broker.get_assets(datetime.date(2022, 3, 7))
            self.assertAlmostEqual(assets, broker.cash + 10 * 500, 2)

        broker = Broker("test", 1e6, 1e-4, start, end)
        broker._assets = np.array(
            [
                (mar1, 1e4),
                (mar2, 1e5),
                (mar3, 1e6),
                (mar4, 1e7),
                (mar7, 1e8),
                (mar9, 1e9),
            ],
            dtype=assets_dtype,
        )

        assets = await broker.get_assets(mar4)
        self.assertEqual(assets, 1e7)

        assets = await broker.get_assets(datetime.date(2022, 3, 5))
        self.assertEqual(assets, 1e7)

    async def test_before_trade(self):
        """this also test get_cash"""
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, start, end)
        hljh = "002537.XSHE"
        tyst = "603717.XSHG"

        await broker.buy(hljh, 9.13, 500, datetime.datetime(2022, 3, 1, 9, 31))
        await broker.buy(hljh, 10.03, 500, datetime.datetime(2022, 3, 4, 9, 31))
        await broker.buy(tyst, 14.84, 1500, datetime.datetime(2022, 3, 7, 9, 31))

        self.assertEqual(6, broker._assets.size)

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
                datetime.date(2022, 2, 28),
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
            [0, 500, 500, 500, 1000, 1000, 1500], broker._positions["shares"].tolist()
        )

        self.assertListEqual(
            [0, 0, 500, 500, 500, 1000, 0], broker._positions["sellable"].tolist()
        )

    async def test_get_position(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, start, end)
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
    async def test_forward_assets(self, mocked_now):
        bt_start = datetime.date(2022, 3, 1)
        bt_stop = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, bt_start, bt_stop)

        # 1. 初始化时就应该有基础assets，方便计算日收益率
        self.assertEqual(1, len(broker._assets))

        # 2. 未进行任何交易，更新到bt_start
        await broker._forward_assets(bt_start)
        self.assertEqual(datetime.date(2022, 2, 28), broker._assets[0]["date"])
        self.assertEqual(1, broker._assets.size)

        self.assertListEqual([1e6], broker._assets["assets"].tolist())

        # 3. 未有交易，更新到超过现金表的某一天
        await broker._forward_assets(datetime.date(2022, 3, 4))
        self.assertEqual(1, broker._assets.size)

        broker._cash = np.concatenate(
            (
                broker._cash,
                np.array(
                    [
                        (datetime.date(2022, 3, 1), 1.0e6),
                        (datetime.date(2022, 3, 2), 1.0e6),
                    ],
                    dtype=cash_dtype,
                ),
            )
        )

        broker._positions = np.concatenate(
            (
                broker._positions,
                np.array(
                    [
                        (datetime.date(2022, 3, 1), None, 0, 0, 0),
                        (datetime.date(2022, 3, 2), None, 0, 0, 0),
                    ],
                    dtype=daily_position_dtype,
                ),
            )
        )

        ## assets date is aligned with cash/positions
        await broker._forward_assets(datetime.date(2022, 3, 4))
        self.assertEqual(datetime.date(2022, 3, 2), broker._assets[-1]["date"])
        self.assertEqual(1.0e6, broker._assets[-1]["assets"])

        # 4. 进行了一次交易，检查是否更新
        broker = Broker("test", 1e6, 1e-4, bt_start, bt_stop)

        broker._cash = np.array(
            [
                (datetime.date(2022, 2, 28), 1.0e6),
                (datetime.date(2022, 3, 1), 1.0e6),
                (datetime.date(2022, 3, 2), 1.0e6 - 500 * 10),
            ],
            dtype=cash_dtype,
        )

        ## 模拟买入hljh 500
        broker._positions = np.array(
            [
                (datetime.date(2022, 2, 28), None, 0, 0, 0),
                (datetime.date(2022, 3, 1), None, 0, 0, 0),
                (datetime.date(2022, 3, 2), hljh, 500, 0, 10),
            ],
            dtype=daily_position_dtype,
        )

        await broker._forward_assets(datetime.date(2022, 3, 2))
        self.assertEqual(datetime.date(2022, 3, 2), broker._assets[-1]["date"])
        self.assertAlmostEqual(1000225, broker._assets[-1]["assets"], 2)

        ## 模拟买入tyst，但没有更新cash表，这不是单测的重点
        broker._positions = np.concatenate(
            (
                broker._positions,
                np.array(
                    [(datetime.date(2022, 3, 2), tyst, 1000, 0, 8.5)],
                    dtype=daily_position_dtype,
                ),
            )
        )
        await broker._forward_assets(datetime.date(2022, 3, 2))
        self.assertAlmostEqual(1015105, broker._assets[-1]["assets"], 2)

        # 5. 换一天再买入hljh，更新assets
        broker._cash = np.concatenate(
            (broker._cash, np.array([(mar3, 1.0e6), (mar4, 1.0e6)], dtype=cash_dtype))
        )
        broker._positions = np.concatenate(
            (
                broker._positions,
                np.array(
                    [(mar3, hljh, 1000, 0, 20), (mar4, tyst, 500, 0, 15)],
                    dtype=daily_position_dtype,
                ),
            )
        )

        await broker._forward_assets(mar4)
        ## should have assets up to 2022, 3, 4
        self.assertEqual(5, broker._assets.size)

        exp = np.array(
            [
                (feb28, 1000000.0),
                (mar1, 1000000.0),
                (mar2, 1015105.0),
                (mar3, 1010610.0),
                # 只有tyst 500股，收盘价14.84
                (mar4, 1007420.0),
            ],
            dtype=assets_dtype,
        )

        np.testing.assert_array_equal(exp["date"], broker._assets["date"])
        np.testing.assert_array_almost_equal(
            exp["assets"], broker._assets["assets"], decimal=2
        )

        # 6. 停牌处理，一个从头停，一个中间停（使用前收）
        ## 1 买入两只，2号tyst停牌（无数据）hljh加仓，3号都无操作,4号卖出hljh
        broker = Broker("test", 1e6, 1e-4, bt_start, bt_stop)

        broker._cash = np.array(
            [
                (feb28, 1e6),
                (mar1, 0.9e6),
                (mar2, 0.9e6),
                (mar3, 0.9e6),
                (mar4, 0.9e6),
            ],
            dtype=cash_dtype,
        )

        broker._positions = np.array(
            [
                (feb28, None, 0, 0, 0),
                (mar1, hljh, 1000, 0, 9.5),
                (mar1, tyst, 500, 0, 9.5),
                (mar2, hljh, 1000, 1000, 9.5),
                (mar2, tyst, 500, 500, 9.5),
                (mar3, hljh, 1000, 1000, 9.5),
                (mar3, tyst, 500, 500, 9.5),
                (mar4, hljh, 500, 500, 9.5),
                (mar4, tyst, 500, 500, 9.5),
            ],
            dtype=daily_position_dtype,
        )

        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                tyst: np.array(
                    [(mar1, 10.45)],  # 3月2日起一直停牌
                    dtype=[("frame", "datetime64[s]"), ("close", "<f4")],
                ),
                hljh: np.array(
                    [
                        (mar1, 9.67),
                    ],
                    dtype=[("frame", "datetime64[s]"), ("close", "<f4")],
                ),
            }.items()

            await broker._forward_assets(mar1)
            self.assertAlmostEqual(914895, broker._assets[-1]["assets"], 2)

        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                tyst: np.array(
                    [(mar1, 10.45)],  # 3月2日起一直停牌
                    dtype=[("frame", "datetime64[s]"), ("close", "<f4")],
                ),
                hljh: np.array(
                    [(mar1, 9.67), (mar2, 9.67), (mar4, 9.68)],  # 3号停牌
                    dtype=[("frame", "datetime64[s]"), ("close", "<f4")],
                ),
            }.items()

            ## tyst从3月2日起停牌 hljh 3月3日起停牌
            await broker._forward_assets(mar4)
            exp = np.array(
                [
                    (feb28, 1e6),
                    (mar1, 914895),
                    (mar2, 914895),
                    (mar3, 914895),
                    (mar4, 910065),
                ],
                dtype=assets_dtype,
            )

            np.testing.assert_array_equal(exp["date"], broker._assets["date"])
            np.testing.assert_array_almost_equal(
                exp["assets"], broker._assets["assets"], decimal=2
            )

        # issue 36
        broker = Broker("test", 1e6, 1e-4, mar1, mar14)
        dr = pd.DataFrame(
            [
                (mar1, 1, 1),
                (mar2, 2, 1),
                (mar3, 2, 1.5),
                (mar4, 2, 1.5),
                (mar7, 2, 1.5),
                (mar8, 2, 1.5),
            ],
            columns=["frame", hljh, tyst],
        )
        dr.set_index("frame", inplace=True)
        with mock.patch(
            "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor"
        ) as mocked:
            mocked.return_value = dr
            await broker.buy(hljh, 9.5, 1000, datetime.datetime(2022, 3, 1, 9, 30))
            await broker.buy(tyst, 15.45, 1000, datetime.datetime(2022, 3, 1, 9, 30))
            await broker.sell(hljh, 9.12, 500, datetime.datetime(2022, 3, 8, 9, 30))

            await broker._forward_assets(mar8)
            exp = np.array(
                [
                    (datetime.date(2022, 2, 28), 1000000.0),
                    (datetime.date(2022, 3, 1), 1000097.52),
                    (datetime.date(2022, 3, 2), 1011057.52),
                    (datetime.date(2022, 3, 3), 1018817.52),
                    (datetime.date(2022, 3, 4), 1016717.52),
                    (datetime.date(2022, 3, 7), 1016952.52),
                    (datetime.date(2022, 3, 8), 1015787.05),
                ],
                dtype=[("date", "O"), ("assets", "<f8")],
            )
            np.testing.assert_array_almost_equal(
                exp["assets"], broker._assets["assets"], 2
            )

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

        broker._bt_stopped = True

        with self.assertRaises(AccountStoppedError):
            await broker._calendar_validation(tf.combine_time(start, 15))

        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1_000_000, 1e-4, start, end)

        await broker._calendar_validation(datetime.datetime(2022, 3, 4, 9, 32))

        with self.assertRaises(TimeRewindError) as cm:
            await broker._calendar_validation(datetime.datetime(2022, 3, 4, 9, 31))
            self.assertTrue(isinstance(cm, TimeRewindError))

        await broker._calendar_validation(datetime.datetime(2022, 3, 4, 9, 33))
        with self.assertRaises(BadParamsError) as cm:
            await broker._calendar_validation(datetime.datetime(2022, 4, 1, 9))

        with self.assertRaises(BadParamsError) as cm:
            await broker._calendar_validation(datetime.datetime(2022, 2, 17, 9))

    async def test_forward_cashtable(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1_000_000, 1e-4, start, end)

        broker._forward_cashtable(tf.day_shift(start, -3))
        self.assertEqual(1, len(broker._cash))
        self.assertEqual(datetime.date(2022, 2, 28), broker._cash[0]["date"])

        broker._forward_cashtable(tf.day_shift(start, 2))
        self.assertEqual(4, len(broker._cash))
        broker._forward_cashtable(tf.day_shift(start, 20))
        self.assertEqual(11, len(broker._cash))

    async def test_forward_positions(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1_000_000, 1e-4, start, end)

        self.assertEqual(1, len(broker._positions))
        self.assertTrue(broker._positions[0]["security"] is None)

        hljh = "002537.XSHE"
        tyst = "603717.XSHG"

        broker._positions = np.array(
            [
                (datetime.date(2022, 2, 28), None, 0.0, 0.0, 0.0),
                (datetime.date(2022, 3, 1), None, 0.0, 0.0, 0.0),
                (datetime.date(2022, 3, 1), hljh, 500.0, 0.0, 9.27),
                (datetime.date(2022, 3, 1), tyst, 1500.0, 0.0, 15.45),
            ],
            dtype=daily_position_dtype,
        )

        frames = [datetime.date(2022, 3, i) for i in (1, 2, 3, 4, 7)]
        mocked_dr_info = pd.DataFrame(
            {hljh: [1.0] + [1.1] * 4, tyst: np.array([1.0, 1.0, 1.2, 1.2, 1.2])},
            index=frames,
        )

        with mock.patch("arrow.now", return_value=datetime.datetime(2022, 3, 14, 15)):
            with mock.patch(
                "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor",
                return_value=mocked_dr_info,
            ):
                await broker._forward_positions(datetime.date(2022, 3, 7))
                exp_hljh = [500, 550, 550, 550, 550]
                actual_hljh = broker._positions["shares"][
                    broker._positions["security"] == hljh
                ]
                np.testing.assert_almost_equal(exp_hljh, actual_hljh)

                exp_tyst = [1500, 1500, 1800, 1800, 1800]
                actual_tyst = broker._positions["shares"][
                    broker._positions["security"] == tyst
                ]
                np.testing.assert_almost_equal(exp_tyst, actual_tyst)

                self.assertEqual(2, len(broker.trades))
                with self.assertRaisesRegexp(KeyError, "found!"):
                    for _, v in broker.trades.items():
                        if (
                            v.security == hljh
                            and v.shares == 50
                            and v.time == datetime.datetime(2022, 3, 2, 15)
                        ):
                            raise KeyError("found!")

        # issue 9, 对持仓为0的股，不查询价格和dr信息
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test_forward_positions", 1_000_000, 1e-4, start, end)

        broker._positions = np.array(
            [
                (datetime.date(2022, 2, 28), None, 0.0, 0.0, 0.0),
                (datetime.date(2022, 3, 1), None, 0.0, 0.0, 0.0),
                (datetime.date(2022, 3, 1), hljh, 500.0, 0.0, 9.27),
                (datetime.date(2022, 3, 4), hljh, 0, 0, 0),
            ],
            dtype=daily_position_dtype,
        )

        with mock.patch(
            "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor"
        ) as mocked:
            await broker._forward_positions(datetime.date(2022, 3, 10))
            np.testing.assert_array_equal(
                broker._positions["date"][4:],
                [datetime.date(2022, 3, i) for i in (7, 8, 9, 10)],
            )
            mocked.assert_not_called()

        broker = Broker("test_forward_positions", 1_000_000, 1e-4, start, end)

        broker._positions = np.array(
            [
                (datetime.date(2022, 2, 28), None, 0.0, 0.0, 0.0),
                (datetime.date(2022, 3, 1), None, 0.0, 0.0, 0.0),
                (datetime.date(2022, 3, 1), hljh, 500.0, 0.0, 9.27),
                (datetime.date(2022, 3, 1), tyst, 500.0, 0.0, 9.27),
                (datetime.date(2022, 3, 4), hljh, 0, 0, 0),
                (datetime.date(2022, 3, 4), tyst, 500.0, 0.0, 9.27),
            ],
            dtype=daily_position_dtype,
        )

        frames = [datetime.date(2022, 3, i) for i in (1, 4, 7, 8, 9)]
        mocked_dr_info = pd.DataFrame({tyst: [1, 1, 1.2, 1.2, 1.2]}, index=frames)

        exp_hljh = np.array(
            [
                (datetime.date(2022, 3, 1), "002537.XSHE", 500.0, 0.0, 9.27),
                (datetime.date(2022, 3, 4), "002537.XSHE", 0.0, 0.0, 0.0),
            ],
            dtype=daily_position_dtype,
        )

        exp_tyst = np.array(
            [
                (datetime.date(2022, 3, 1), "603717.XSHG", 500.0, 0.0, 9.27),
                (datetime.date(2022, 3, 4), "603717.XSHG", 500.0, 0.0, 9.27),
                (datetime.date(2022, 3, 7), "603717.XSHG", 600.0, 500.0, 7.725),
                (datetime.date(2022, 3, 8), "603717.XSHG", 600.0, 600.0, 7.725),
                (datetime.date(2022, 3, 9), "603717.XSHG", 600.0, 600.0, 7.725),
            ],
            dtype=daily_position_dtype,
        )

        with mock.patch(
            "backtest.feed.zillionarefeed.ZillionareFeed.get_dr_factor",
            return_value=mocked_dr_info,
        ):
            await broker._forward_positions(datetime.date(2022, 3, 9))
            actual_hljh = broker._positions[
                broker._positions["security"] == "002537.XSHE"
            ]
            actual_tyst = broker._positions[
                broker._positions["security"] == "603717.XSHG"
            ]
            for key in ("shares", "sellable", "price"):
                np.testing.assert_almost_equal(exp_hljh[key], actual_hljh[key], 2)
                np.testing.assert_almost_equal(exp_tyst[key], actual_tyst[key])

    @pytest.mark.skip(os.environ.get("IS_GITHUB"))
    async def test_issue_with_local_omicron(self):
        try:
            config_dir = os.path.expanduser("~/zillionare/notebook")
            cfg = cfg4py.init(config_dir)

            await omicron.close()
            await omicron.init()
            await emit.start(emit.Engine.REDIS, start_server=True, dsn=cfg.redis.dsn)

            self.ctx = get_app_context()
            self.ctx.feed = ZillionareFeed()
            await self.ctx.feed.init()

            start = datetime.date(2022, 10, 1)
            end = datetime.date(2022, 10, 31)

            broker = Broker("test", 1_000_000, 1e-4, start, end)
            await broker.buy(
                "300539.XSHE", 9.26, 1000, datetime.datetime(2022, 10, 10, 9, 31)
            )
            await broker.sell(
                "300539.XSHE", 8.98, 1000, datetime.datetime(2022, 10, 21, 9, 30)
            )

        finally:
            await emit.stop()
            await omicron.close()

    async def test_query_market_values(self):
        broker = Broker("test", 1_000_000, 1e-4, mar1, mar14)
        positions = np.array(
            [
                (mar1, tyst, 500, 0, 0),
                (mar2, tyst, 1000, 500, 0),
                (mar3, hljh, 500, 500, 0),
                (mar3, tyst, 1000, 1000, 0),
                (mar4, hljh, 500, 500, 0),
                (mar7, None, 0, 0, 0),
            ],
            dtype=daily_position_dtype,
        )

        tyst_bars = np.array(
            [(mar1, 10), (mar3, 8), (mar4, 7), (mar7, 6), (mar8, 5)],  # 3月2日起停牌一天
            dtype=[("frame", "datetime64[s]"), ("close", "<f4")],
        )
        hljh_bars = np.array(
            [(mar1, 10), (mar2, 20), (mar4, 30), (mar7, 30), (mar8, 30)],  # 3号停牌
            dtype=[("frame", "datetime64[s]"), ("close", "<f4")],
        )

        # 1. forward to mar2 and query to mar2
        broker._positions = positions[:2]
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                tyst: tyst_bars[:1],
                hljh: hljh_bars[:2],
            }.items()
            mv = await broker._query_market_values(mar1, mar2)
            self.assertListEqual([5000, 10000], mv.tolist())

        # 2. forward to mar3 and query to mar2
        broker._positions = positions[:4]
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                tyst: tyst_bars[:1],
                hljh: hljh_bars[:2],
            }.items()
            mv = await broker._query_market_values(mar1, mar2)
            self.assertListEqual([5000, 10000], mv.tolist())

        # 4. forwar to mar7, query to mar4
        broker._positions = positions
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                tyst: tyst_bars[:3],
                hljh: hljh_bars[:3],
            }.items()
            mv = await broker._query_market_values(mar1, mar4)
            self.assertListEqual([5000, 10000, 18000, 15000], mv.tolist())

        # 5. forward to mar7 and query to mar7
        broker._positions = positions
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                tyst: tyst_bars[:-1],
                hljh: hljh_bars[:-1],
            }.items()
            mv = await broker._query_market_values(mar1, mar7)
            self.assertListEqual([5000, 10000, 18000, 15000, 0], mv.tolist())

        # 6. forward to mar14 (backtest end) and query to mar8
        broker._positions = positions
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                tyst: tyst_bars,
                hljh: hljh_bars,
            }.items()
            mv = await broker._query_market_values(mar1, mar8)
            self.assertListEqual([5000, 10000, 18000, 15000, 0, 0], mv.tolist())

        # 7. forward to mar14 (backtest end) and query to mar14
        broker._positions = positions
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                tyst: tyst_bars,
                hljh: hljh_bars,
            }.items()
            mv = await broker._query_market_values(mar1, mar14)
            self.assertListEqual(
                [5000, 10000, 18000, 15000, 0, 0, 0, 0, 0, 0], mv.tolist()
            )

    async def test_bills(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, start, end)
        hljh = "002537.XSHE"

        await broker.buy(hljh, 9.13, 500, datetime.datetime(2022, 3, 1, 9, 31))
        await broker.sell(hljh, 9.1, 100, datetime.datetime(2022, 3, 14, 15))

        await broker.stop_backtest()
        bills = broker.bills()

        tx = bills["tx"][0]
        self.assertEqual(tx.shares, 100)
        self.assertAlmostEqual(tx.fee, 0.19, 2)
        self.assertEqual(tx.sec, hljh)
        self.assertEqual(tx.window, 10)
        self.assertAlmostEqual(tx.entry_price, 9.09, 2)
        self.assertAlmostEqual(tx.exit_price, 9.56, 2)
        self.assertEqual(tx.exit_time, datetime.datetime(2022, 3, 14, 15))

        trades = bills["trades"]
        for _, v in trades.items():
            if v.side == EntrustSide.BUY:
                self.assertAlmostEqual(v.fee, 0.45, 2)
                self.assertAlmostEqual(v.price, 9.09, 2)
                self.assertEqual(v.shares, 500)
                self.assertEqual(v._unsell, 400)
                self.assertAlmostEqual(v._unamortized_fee, 0.36, 0)
            if v.side == EntrustSide.SELL:
                self.assertAlmostEqual(v.fee, 0.1, 2)
                self.assertAlmostEqual(v.price, 9.56, 2)
                self.assertAlmostEqual(v.shares, 100)

        positions = bills["positions"]
        np.testing.assert_array_equal(positions["shares"], [0, *([500] * 9), 400])

    async def test_match_bid(self):
        # 仅使用frame
        bars = bars_from_csv("hljh", "1m", 2, 241)[["frame", "close", "volume"]].astype(
            match_data_dtype
        )[:20]
        bars["price"] = [10 + i / 100 for i in range(20)]
        bars["volume"] = np.arange(1, 21)

        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, start, end)

        # 1. 直到当天结束，都没有足够的票
        mp, filled, frame = broker._match_bid(bars, 300)
        self.assertAlmostEqual(mp, 10.1235, 2)
        self.assertEqual(filled, 200)
        self.assertEqual(frame, datetime.datetime(2022, 3, 1, 9, 50))

        # 2. 当天未结束即凑够
        mp, filled, frame = broker._match_bid(bars, 100)
        self.assertAlmostEqual(mp, 10.08, 2)
        self.assertEqual(filled, 100)
        self.assertEqual(frame, datetime.datetime(2022, 3, 1, 9, 44))

    def test_remove_for_buy(self):
        order_time = datetime.datetime(2022, 3, 1, 9, 31)
        bars = bars_from_csv("hljh", "1m", 2, 241)
        bars = bars[["frame", "close", "volume"]].astype(match_data_dtype)
        buy_limit_price = 9.5
        sell_limit_price = 9.09

        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        broker = Broker("test", 1e6, 1e-4, start, end)

        bars = broker._remove_for_buy(
            hljh, order_time, bars, 9.22, buy_limit_price, sell_limit_price
        )
        self.assertEqual(10, len(bars))

        # where meet sell limit is changed to 1e20
        self.assertAlmostEqual(1e20, bars[3]["volume"])

        # 2. 全部为涨停价
        bars["price"] = 9
        with self.assertRaises(BuylimitError):
            bars = broker._remove_for_buy(
                hljh, order_time, bars, 9.22, 9, sell_limit_price
            )
            pass

    def test_remove_for_sell(self):
        start = datetime.date(2022, 3, 1)
        end = datetime.date(2022, 3, 14)
        order_time = datetime.datetime(2022, 3, 1, 9, 31)

        broker = Broker("test", 1e6, 1e-4, start, end)

        buy_limit_price = 9.5
        sell_limit_price = 9.09

        # 1. 部分触及跌停
        bars = bars_from_csv("hljh", "1m", 2, 241)
        bars = bars[["frame", "close", "volume"]].astype(match_data_dtype)
        bars["price"][20:] = 9.09
        bars["price"][3] = buy_limit_price
        bars = broker._remove_for_sell(
            hljh, order_time, bars, 9.10, sell_limit_price, buy_limit_price
        )
        self.assertEqual(20, len(bars))
        self.assertEqual(1e20, bars[3]["volume"])

        # 2. 全部为跌停价
        bars["price"] = sell_limit_price
        with self.assertRaises(SellLimitError):
            bars = broker._remove_for_sell(
                hljh, order_time, bars, 9.22, sell_limit_price, buy_limit_price
            )
            pass
