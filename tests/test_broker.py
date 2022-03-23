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
from backtest.trade.types import EntrustError, EntrustSide, position_dtype
from tests import create_file_feed

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
        self.ctx.feed = create_file_feed()
        await self.ctx.feed.init()

        return await super().asyncSetUp()

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

    async def test_buy(self):
        global bars

        tyst = "603717.XSHG"
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

        market_value = shares1 * close_price_of_the_day
        assets = cash + market_value

        positions = np.array([(hljh, shares1, 0, price1)], dtype=position_dtype)
        self._check_position(broker, positions, datetime.datetime(2022, 3, 10, 9, 35))
        self.assertAlmostEqual(assets, broker.assets, 2)
        self.assertAlmostEqual(cash, broker.cash)

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
        positions = np.array([(hljh, shares, 0, price)], dtype=position_dtype)

        cash = start_cash - price2 * shares2 * (1 + commission)
        assets = cash + shares * close_price_of_the_day

        self.assertAlmostEqual(assets, broker.assets, 2)
        self.assertAlmostEqual(cash, broker.cash, 2)
        self._check_position(broker, positions, datetime.datetime(2022, 3, 10, 9, 35))

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

        self.assertAlmostEqual(assets, broker.assets, 2)
        self.assertAlmostEqual(cash, broker.cash, 2)
        self._check_position(broker, positions, bid_time)

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

        mar_7 = datetime.datetime(2022, 3, 7, 9, 41)
        mar_8 = datetime.datetime(2022, 3, 8, 14, 8)
        mar_9 = datetime.datetime(2022, 3, 9, 9, 40)
        mar10 = datetime.datetime(2022, 3, 10, 9, 33)
        await broker.buy(tyst, 14.84, 500, mar_7)
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
            mar10,
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
        self._check_position(broker, pos, mar10)
        self.assertAlmostEqual(999_073.47, broker.assets, 2)
        self.assertAlmostEqual(974_781.47, broker.cash, 2)

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

        positions = np.array([("002537.XSHE", 2000.0, 1000, 9.02)], position_dtype)
        self._check_position(broker, positions, mar10)
        self.assertAlmostEqual(999_568.93, broker.assets, 2)
        self.assertAlmostEqual(980_208.93, broker.cash, 2)

        # 成交量不足撮合委卖
        broker = Broker("test", 1e10, 1e-4)

        await broker.buy(tyst, 14.84, 1e8, datetime.datetime(2022, 3, 7, 9, 41))
        self._check_position(
            broker,
            np.array([(tyst, 802700, 802700, 14.79160334)], position_dtype),
            mar10,
        )

        result = await broker.sell(
            tyst, 12.33, 1e8, datetime.datetime(2022, 3, 10, 9, 35)
        )

        self.assertEqual(EntrustError.SUCCESS, result["status"])
        self.assertEqual(0, len(broker.position))
        self.assertAlmostEqual(9_998_678_423.29, broker.assets, 2)
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

    def test_get_assets(self):
        broker = Broker("test", 1e7, 1e-4)

        self.assertEqual(broker.capital, broker.get_assets(datetime.date(2022, 3, 1)))

        broker._assets = {
            datetime.date(2022, 3, 1): 1e6,
            datetime.date(2022, 3, 2): 1e5,
            datetime.date(2022, 3, 15): 1e4,
        }

        self.assertEqual(1e6, broker.get_assets(datetime.date(2022, 3, 1)))
        self.assertEqual(1e5, broker.get_assets(datetime.date(2022, 3, 2)))
        self.assertEqual(1e5, broker.get_assets(datetime.date(2022, 3, 8)))
        self.assertEqual(1e4, broker.get_assets(datetime.date(2022, 3, 15)))
        self.assertEqual(1e4, broker.get_assets(datetime.date(2022, 3, 16)))

    def test_update_positions(self):
        """this also tests get_positions"""
        broker = Broker("test", 1e7, 1e-4)
        tyst, hljh = "603717.XSHG", "002537.XSHE"

        mar_7 = datetime.datetime(2022, 3, 7, 9, 41)
        mar_8 = datetime.datetime(2022, 3, 8, 14, 8)
        mar_9 = datetime.datetime(2022, 3, 9, 9, 40)
        mar_10 = datetime.datetime(2022, 3, 10, 9, 33)

        trade = Trade("1", tyst, 14.84, 500, 1.5, EntrustSide.BUY, mar_7)

        broker._update_position(trade, mar_7)

        positions_37 = np.array([(tyst, 500, 0, 14.84)], position_dtype)

        self._check_position(broker, positions_37, mar_7)
        positions_38 = positions_37.copy()
        positions_38["sellable"][0] = 500

        # 经过一天后，所有持仓都可以卖出
        self._check_position(broker, positions_38, mar_8)

        # 再买入一笔，计算平均成本
        trade = Trade("2", tyst, 16, 1000, 4, EntrustSide.BUY, mar_8)
        broker._update_position(trade, mar_8)

        # 3月7日持仓不变
        self._check_position(broker, positions_37, mar_7)

        positions_38 = np.array([(tyst, 1500, 500, 15.613333333333333)], position_dtype)
        self._check_position(broker, positions_38, mar_8)

        # 3月9日，所有仓位变为可卖
        position_39 = positions_38.copy()
        position_39["sellable"] = 1500
        self._check_position(broker, position_39, mar_9)

        # 再买入其它股票
        trade = Trade("3", hljh, 9.94, 1500, 1.5, EntrustSide.BUY, mar_8)
        broker._update_position(trade, mar_8)
        positions = np.array(
            [
                (tyst, 1500, 500, 15.613333333333333),
                (hljh, 1500, 0, 9.94),
            ],
            dtype=position_dtype,
        )

        self._check_position(broker, positions, mar_8)
        positions[0]["sellable"] = 1500
        positions[1]["sellable"] = 1500

        self._check_position(broker, positions, mar_9)

        # 卖出一半hljh
        trade = Trade("4", hljh, 9.94, 750, 1.5, EntrustSide.SELL, mar_10)
        broker._update_position(trade, mar_10)

        positions = np.array(
            [(tyst, 1500, 1500, 15.613333333333333), (hljh, 750, 750, 9.94)],
            dtype=position_dtype,
        )

        self._check_position(broker, positions, mar_10)

        # tyst全部卖出
        trade = Trade("5", tyst, 14.84, 1500, 1.5, EntrustSide.SELL, mar_10)
        broker._update_position(trade, mar_10)

        positions = np.array(
            [
                (hljh, 750, 750, 9.94),
            ],
            dtype=position_dtype,
        )
        self._check_position(broker, positions, mar_10)

    async def test_metrics(self):
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

        actual = broker.metrics()
        exp = {
            "start": datetime.datetime(2022, 3, 1, 9, 31),
            "end": datetime.datetime(2022, 3, 14, 15),
            "window": 10,
            "total_tx": 9,
            "total_profit": -779.16,
            "win_rate": 0.444,
            "max_drawdown": -0.00826,
            "mean_return": -0.00055013,
            "sharpe": -303.672,
            "sortino": -15.86,
            "calmar": -15.7,
            "annual_return": -0.1297,
            "volatility": 0.0253,
        }

        for k in [
            "start",
            "end",
            "window",
            "total_tx",
            "total_profit",
            "win_rate",
            "max_drawdown",
            "mean_return",
            "sharpe",
            "sortino",
            "calmar",
            "annual_return",
            "volatility",
        ]:
            v = exp[k]
            if type(v) == float:
                self.assertAlmostEqual(v, actual[k], 2)
            else:
                self.assertEqual(v, actual[k])
