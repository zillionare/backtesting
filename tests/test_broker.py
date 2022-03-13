import datetime
import os
import unittest
from unittest import mock

import cfg4py
import numpy as np
import omicron
from sanic import Sanic

from backtest.broker import Broker
from backtest.config import get_config_dir
from backtest.feed.filefeed import FileFeed
from backtest.helper import get_app_context
from backtest.trade import Trade
from backtest.types import BidType, Entrust, EntrustError, EntrustSide
from tests import data_dir

app = Sanic("backtest")


class BrokerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        global app
        cfg4py.init(get_config_dir())

        await omicron.init()

        self.ctx = get_app_context()
        match_bars_file = os.path.join(data_dir(), "bars_match.pkl")
        price_limits_file = os.path.join(data_dir(), "price_limits.pkl")
        self.ctx.feed = FileFeed(match_bars_file, price_limits_file)
        await self.ctx.feed.init()

        return await super().asyncSetUp()

    def check_order_result(self, actual, status, sec, price, shares, commission):
        self.assertEqual(actual["status"], status)
        self.assertEqual(actual["data"]["security"], sec)
        self.assertAlmostEqual(actual["data"]["price"], price)
        self.assertEqual(actual["data"]["shares"], shares)
        self.assertEqual(actual["data"]["fee"], price * shares * commission)
        self.assertIsNotNone(actual["data"]["tid"])

    def check_balance(self, broker, cash, assets, positions):
        self.assertAlmostEqual(broker.cash, cash)
        self.assertAlmostEqual(broker.current_assets, assets, 2)
        self.assertEqual(len(broker.current_positions), len(positions))

        for key, value in broker.current_positions.items():
            self.assertEqual(value[0], positions[key][0])
            self.assertAlmostEqual(value[1], positions[key][1], 2)
            self.assertEqual(value[2], positions[key][2])

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
        self.check_order_result(
            result, EntrustError.PARTIAL_SUCCESS, hljh, price1, shares1, commission
        )

        spend = price1 * shares1 * (1 + commission)
        cash = broker.capital - spend

        mv = shares1 * close_price_of_the_day  # market_value
        assets = cash + mv
        positions = {hljh: [hljh, shares1, price1]}
        self.check_balance(broker, cash, assets, positions)

        # 委买当笔即全部成交
        start_cash = broker.cash  # 9240417581.183184

        result = await broker.buy(
            hljh,
            9.43,
            1e5,
            datetime.datetime(2022, 3, 10, 9, 35),
        )

        price2, shares2, close_price_of_the_day = 9.12, 1e5, 9.68

        self.check_order_result(
            result, EntrustError.SUCCESS, hljh, price2, shares2, commission
        )

        shares = shares1 + shares2
        price = (price1 * shares1 + price2 * shares2) / shares
        positions = {hljh: [hljh, shares, price]}

        cash = start_cash - price2 * shares2 * (1 + commission)
        assets = cash + shares * close_price_of_the_day

        self.check_balance(broker, cash, assets, positions)

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
            1, "002357.XSHE", 1.0, 100, 5, EntrustSide.BUY, datetime.date(2022, 3, 3)
        )
        broker.trades[trade.tid] = trade
        broker._append_unclosed_trades(trade.tid, datetime.date(2022, 3, 3))

        positions = broker.get_positions(datetime.date(2022, 3, 3))
        self.assertEqual(1, len(positions))
        self.assertTupleEqual(("002357.XSHE", 100, 1.0), positions[0])

        positions = broker.get_positions(datetime.date(2022, 3, 7))
        self.assertEqual(3, len(broker._unclosed_trades))
        self.assertEqual(1, len(positions))
        self.assertTupleEqual(("002357.XSHE", 100, 1.0), positions[0])

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

        self.assertEqual(5, len(broker._unclosed_trades))
        positions = broker.get_positions(datetime.date(2022, 3, 9))

        self.assertEqual(2, len(positions))
        if positions[0][0] == "603717.XSHG":
            self.assertTupleEqual(("603717.XSHG", 2000, 1.5), positions[0])
        else:
            self.assertTupleEqual(("603717.XSHG", 2000, 1.5), positions[1])

    async def test_current_positions(self):
        broker = Broker("test", 1e10, 1e-4)

        trade = Trade(
            1, "002357.XSHE", 1.0, 100, 5, EntrustSide.BUY, datetime.date(2022, 3, 3)
        )
        broker.trades[trade.tid] = trade
        broker._append_unclosed_trades(trade.tid, datetime.date(2022, 3, 3))

        positions = broker.current_positions
        self.assertEqual(1, len(positions))
        self.assertTupleEqual(("002357.XSHE", 100, 1.0), positions[0])

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

        positions = broker.current_positions

        self.assertEqual(2, len(positions))
        if positions[0][0] == "603717.XSHG":
            self.assertTupleEqual(("603717.XSHG", 2000, 1.5), positions[0])
        else:
            self.assertTupleEqual(("603717.XSHG", 2000, 1.5), positions[1])

    async def test_get_unclosed_trades(self):
        broker = Broker("test", 1e10, 1e-4)

        self.assertEqual(0, len(broker.get_unclosed_trades(datetime.date(2022, 3, 3))))

        broker._append_unclosed_trades(0, datetime.date(2022, 3, 3))
        self.assertSetEqual({0}, broker.get_unclosed_trades(datetime.date(2022, 3, 3)))

        self.assertSetEqual({0}, broker.get_unclosed_trades(datetime.date(2022, 3, 4)))
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
        self.assertSetEqual({0}, broker._unclosed_trades[datetime.date(2022, 3, 3)])
        self.assertSetEqual(
            {0, 1, 2, 3}, broker._unclosed_trades[datetime.date(2022, 3, 10)]
        )

    async def test_sell(self):
        broker = Broker("test", 1e6, 1e-4)
        tyst, hljh = "603717.XSHG", "002537.XSHE"

        await broker.buy(tyst, 14.84, 500, datetime.datetime(2022, 3, 7, 9, 41))
        await broker.buy(tyst, 14.79, 1000, datetime.datetime(2022, 3, 8, 14, 8))
        await broker.buy(hljh, 8.95, 1000, datetime.datetime(2022, 3, 9, 9, 40))
        await broker.buy(hljh, 9.09, 1000, datetime.datetime(2022, 3, 10, 9, 33))

        # 可用余额足够，当前买单足够
        price, shares, time = 12.98, 1100, datetime.datetime(2022, 3, 10, 9, 35)
        result = await broker.sell(tyst, price, shares, time)

        self.assertEqual(6, len(broker.trades))
        self.assertEqual(3, len(broker._unclosed_trades))
        exit_price, sold_shares = (13.83, 1100)
        result["data"] = result["data"][0]
        self.check_order_result(
            result,
            EntrustError.SUCCESS,
            tyst,
            exit_price,
            sold_shares,
            broker.commission,
        )

        # 可用余额足够，当前买单不足
        order = Entrust(
            None,
            tyst,
            EntrustSide.BUY,
            3 * 10e5,
            9,
            datetime.datetime(2022, 3, 9, 9, 35),
            BidType.LIMIT,
        )
        trade = Trade(order, 10, 3 * 10e8, 5.8)
        broker.positions = {tyst: [trade]}
        broker.cash = 0

        result = await broker.sell(
            tyst, 12.33, 3 * 10e6, datetime.datetime(2022, 3, 10, 9, 35)
        )
        self.assertEqual(result["status"], EntrustError.SUCCESS)
        self.assertEqual(broker.cash, 62907806.59, 2)

        # 可用余额不足，当前买单足够

        # 持股数够，但可用余额不足
