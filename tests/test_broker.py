import asyncio
import datetime
import logging
import unittest
from unittest import mock

import arrow
import cfg4py
import numpy as np
import omicron
from omicron.models.timeframe import TimeFrame as tf
from pyemit import emit

from backtest.common.helper import get_app_context
from backtest.config import get_config_dir
from backtest.feed.zillionarefeed import ZillionareFeed
from backtest.trade.broker import Broker
from backtest.trade.trade import Trade
from backtest.trade.types import (
    E_BACKTEST,
    EntrustError,
    EntrustSide,
    assets_dtype,
    cash_dtype,
    position_dtype,
)
from tests import assert_deep_almost_equal, data_populate

logger = logging.getLogger(__name__)


class BrokerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        cfg = cfg4py.init(get_config_dir())

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
        await omicron.close()
        await emit.stop()
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

    def _check_order_result(self, actual, status, sec, price, shares, commission):
        self.assertEqual(actual["status"], status)

        if isinstance(sec, set):
            self.assertSetEqual(set([v.security for v in actual["data"]]), sec)
        else:
            self.assertEqual(actual["data"].security, sec)

        # exit price would be same
        if isinstance(actual["data"], list):
            for v in actual["data"]:
                self.assertAlmostEqual(v.price, price, 2)

            sum_shares = np.sum([v.shares for v in actual["data"]])
            self.assertEqual(sum_shares, shares)
            sum_fee = np.sum([v.fee for v in actual["data"]])
            self.assertAlmostEqual(sum_fee, price * shares * commission, 2)
        else:
            self.assertAlmostEqual(actual["data"].price, price, 2)

            self.assertEqual(actual["data"].shares, shares)
            self.assertAlmostEqual(actual["data"].fee, price * shares * commission, 2)

    async def test_buy(self):
        tyst = "603717.XSHG"
        hljh, capital, commission = "002537.XSHE", 1e10, 1e-4
        broker = Broker("test", capital, commission)

        async def on_backtest_event(data):
            logger.info("on_backtest_event: %s", data)

        emit.register(E_BACKTEST, on_backtest_event)
        # 委买部分成交
        result = await broker.buy(
            hljh,
            9.43,
            1e9,  # total available shares: 81_840_998
            datetime.datetime(2022, 3, 10, 9, 35),
        )

        price1, shares1, close_price_of_the_day = 9.324918712004743, 29265100.0, 9.68
        self._check_order_result(
            result, EntrustError.PARTIAL_SUCCESS, hljh, price1, shares1, commission
        )

        change = price1 * shares1 * (1 + commission)
        cash = broker.capital - change

        market_value = shares1 * close_price_of_the_day
        assets = cash + market_value

        positions = np.array([(hljh, shares1, 0, price1)], dtype=position_dtype)
        self._check_position(broker, positions, datetime.date(2022, 3, 10))
        self.assertAlmostEqual(assets, broker.assets, 2)
        self.assertAlmostEqual(cash, broker.cash)

        # 委买当笔即全部成交
        start_cash = broker.cash  # 9727078031.93345

        result = await broker.buy(
            hljh, 9.43, 1e5, datetime.datetime(2022, 3, 10, 9, 35)
        )

        price2, shares2, close_price_of_the_day = 9.12, 1e5, 9.68

        self._check_order_result(
            result, EntrustError.SUCCESS, hljh, price2, shares2, commission
        )

        shares = shares1 + shares2
        price = (price1 * shares1 + price2 * shares2) / shares
        positions = np.array([(hljh, shares, 0, price)], dtype=position_dtype)

        cash = start_cash - price2 * shares2 * (1 + commission)
        assets = cash + shares * close_price_of_the_day

        self.assertAlmostEqual(assets, broker.assets, 1)
        self.assertAlmostEqual(cash, broker.cash, 1)
        self._check_position(broker, positions, datetime.date(2022, 3, 10))

        # 买入时已经涨停
        result = await broker.buy(
            hljh, 9.68, 10e4, datetime.datetime(2022, 3, 10, 14, 33)
        )

        self.assertEqual(result["status"], EntrustError.REACH_BUY_LIMIT)

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
        broker._cash = np.array(
            [
                (datetime.date(2022, 3, 11), 100),
            ],
            dtype=cash_dtype,
        )

        result = await broker.buy(
            hljh, 10.20, 10e4, datetime.datetime(2022, 3, 11, 9, 35)
        )

        self.assertEqual(result["status"], EntrustError.NO_CASH)

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
        result = await broker.sell(tyst, bid_price, bid_shares, bid_time)
        self.assertEqual(EntrustError.NO_POSITION, result["status"])

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
            result,
            EntrustError.SUCCESS,
            {tyst},
            exit_price,
            sold_shares,
            broker.commission,
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
        result = await broker.sell(tyst, bid_price, bid_shares, bid_time)
        self.assertEqual(EntrustError.REACH_SELL_LIMIT, result["status"])

        # 余额不足： 尽可能卖出
        bid_price, bid_shares, bid_time = (
            12.33,
            1100,
            datetime.datetime(2022, 3, 10, 9, 35),
        )
        result = await broker.sell(tyst, bid_price, bid_shares, bid_time)

        positions = np.array(
            [
                ("002537.XSHE", 2000.0, 1000, 9.02),
                ("603717.XSHG", 0.0, 0.0, 0),
            ],
            position_dtype,
        )
        self._check_position(broker, positions, mar10.date())
        self.assertAlmostEqual(999_568.93, broker.assets, 2)
        self.assertAlmostEqual(980_208.93, broker.cash, 2)

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

        self.assertEqual(EntrustError.SUCCESS, result["status"])
        self.assertEqual(0, broker.position["shares"].item())
        self.assertAlmostEqual(9998678423.08407, broker.assets, 2)
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
        await broker.buy(hljh, 9.65, 500, datetime.datetime(2022, 3, 11, 9, 31))
        await broker.buy(hljh, 9.65, 500, datetime.datetime(2022, 3, 14, 9, 31))
        await broker.sell(hljh, 9.1, 5000, datetime.datetime(2022, 3, 14, 15))

        actual = await broker.metrics(baseline=hljh)
        exp = {
            "start": datetime.date(2022, 3, 1),
            "end": datetime.date(2022, 3, 14),
            "window": 10,
            "total_tx": 9,
            "total_profit": -779.1568067073822,
            "total_profit_rate": -0.0007791568067073822,
            "win_rate": 0.4444444444444444,
            "mean_return": -6.952228828114507e-05,
            "sharpe": -1.7461,
            "sortino": -2.51418,
            "calmar": -3.9873290,
            "max_drawdown": -0.004438,
            "annual_return": -0.017698244,
            "volatility": 0.02721410,
            "baseline": {
                "code": hljh,
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

        await broker.buy(hljh, 9.13, 500, datetime.datetime(2022, 3, 1, 9, 31))

        for i in range(10):
            dt = tf.day_shift(datetime.date(2022, 3, 1), i)
            actual = await broker.get_assets(dt)
            print(actual)

    async def test_before_trade(self):
        """this also test get_cash"""
        broker = Broker("test", 1e6, 1e-4)
        hljh = "002537.XSHE"
        tyst = "603717.XSHG"

        await broker.buy(hljh, 9.13, 500, datetime.datetime(2022, 3, 1, 9, 31))
        await broker.buy(hljh, 10.03, 500, datetime.datetime(2022, 3, 4, 9, 31))
        await broker.buy(tyst, 14.84, 1500, datetime.datetime(2022, 3, 7, 9, 31))

        self.assertListEqual(
            [
                datetime.date(2022, 3, 1),
                datetime.date(2022, 3, 4),
                datetime.date(2022, 3, 7),
            ],
            broker._assets["date"].tolist(),
        )

        self.assertListEqual(
            [
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

    async def test_get_position(self):
        broker = Broker("test", 1e6, 1e-4)
        hljh = "002537.XSHE"

        self.assertEqual(0, broker.position.size)

        await broker.buy(hljh, 9.13, 500, datetime.datetime(2022, 3, 1, 9, 31))
        self.assertEqual(0, broker.position["sellable"].item())

        # next day, it's all sellable
        sellable = broker.get_position(datetime.date(2022, 3, 4))["sellable"].item()
        self.assertEqual(500, sellable)

        await broker.sell(hljh, 9.59, 500, datetime.datetime(2022, 3, 4, 9, 31))
        self.assertEqual(0, broker.position["shares"].item())

    async def test_recalc_assets(self):
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

        await broker.buy(hljh, 10.03, 500, datetime.datetime(2022, 3, 4, 9, 31))
        await broker.buy(tyst, 14.84, 1500, datetime.datetime(2022, 3, 7, 9, 31))

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

            broker._assets = np.array([], dtype=assets_dtype)
            await broker.sell(hljh, 8.2, 500, datetime.datetime(2022, 3, 7, 9, 31))
            await broker.sell(tyst, 14.28, 1500, datetime.datetime(2022, 3, 8, 9, 31))
            await broker.recalc_assets()

            exp = np.array(
                [
                    (datetime.date(2022, 3, 3), 1e6),
                    (datetime.date(2022, 3, 4), 999864.50717168),
                    (datetime.date(2022, 3, 7), 999891.8144659),
                    (datetime.date(2022, 3, 8), 999754.59475198),
                    (datetime.date(2022, 3, 9), 999754.59475198),
                    (datetime.date(2022, 3, 10), 999754.59475198),
                ],
                dtype=[("date", "O"), ("assets", "<f8")],
            )

            np.testing.assert_array_equal(exp["date"], broker._assets["date"])
            np.testing.assert_array_almost_equal(
                exp["assets"], broker._assets["assets"], decimal=2
            )
