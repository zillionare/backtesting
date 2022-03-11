import datetime
import unittest
from argparse import Namespace
from unittest import mock
from xml.dom.minidom import Entity

import numpy as np
import omicron
from aiohttp import request
from sanic import Sanic

from backtest.broker import Broker
from backtest.data.basefeed import BaseFeed
from backtest.types import BidType, EntrustError, Order, OrderSide, Trade
from tests.data import bars_for_test_buy, bars_for_test_sell


class BrokerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = Sanic("backtest")
        self.app.ctx = Namespace()

        return await super().asyncSetUp()

    def check_order_result(self, actual, status, sec, price, shares, fee):
        self.assertEqual(actual["status"], status)
        self.assertEqual(actual["data"].security, sec)
        self.assertAlmostEqual(actual["data"].price, price)
        self.assertEqual(actual["data"].shares, shares)
        self.assertEqual(actual["data"].fee, fee)
        self.assertIsNotNone(actual["data"].tid)

    def check_balance(self, broker, cash, assets, positions):
        self.assertAlmostEqual(broker.available_cash, cash)
        self.assertAlmostEqual(broker.current_assets, assets, 2)
        self.assertEqual(len(broker.positions), len(positions))

        for key, value in broker.positions.items():
            self.assertEqual(value[0], positions[key][0])
            self.assertAlmostEqual(value[1], positions[key][1], 2)
            self.assertEqual(value[2], positions[key][2])

    async def test_buy(self):
        global bars

        get_limits = "omicron.models.stock.Stock.get_trade_price_limits"
        limits = np.array(
            [(datetime.date(2022, 3, 1), 9.68, 8)],
            dtype=[("frame", "O"), ("high_limit", "<f4"), ("low_limit", "<f4")],
        )

        with mock.patch("backtest.helper.get_app_context", return_value=self.app.ctx):
            self.app.ctx.feed = mock.AsyncMock()
            self.app.ctx.feed.get_bars.return_value = bars_for_test_buy
            self.app.ctx.feed.remove_buy_limit_bars = BaseFeed.remove_buy_limit_bars
            self.app.ctx.feed.get_close_price.return_value = {"002537.XSHE": 9.68}

            code = "002537.XSHE"
            with mock.patch(get_limits, return_value=limits):
                broker = Broker("test", 1000000, 1 / 10000)

                # 委买全部成交
                result = await broker.buy(
                    code,
                    9.43,
                    10e4,
                    datetime.datetime(2022, 3, 10, 9, 35),
                )

                price = 9.1
                shares = 10e4
                fee = 91

                self.check_order_result(
                    result, EntrustError.SUCCESS, code, price, shares, fee
                )

                positions = {code: [code, 9.1, 10e4]}
                available_cash = broker.cash - price * shares - fee
                assets = available_cash + shares * 9.68  # close price of the day

                self.check_balance(broker, available_cash, assets, positions)

                # 委买部分成交
                broker.available_cash = 10e10
                result = await broker.buy(
                    "002537.XSHE",
                    9.43,
                    10e8,  # total available shares: 81840998
                    datetime.datetime(2022, 3, 10, 9, 35),
                )

                price, shares, fee = 9.280280223977718, 81840998, 75950.739525
                self.check_order_result(
                    result, EntrustError.PARTIAL_SUCCESS, code, price, shares, fee
                )

                # 买入时已经涨停
                result = await broker.buy(
                    "002537.XSHE", 9.68, 10e4, datetime.datetime(2022, 3, 10, 14, 33)
                )

                self.assertEqual(result["status"], EntrustError.REACH_BUY_LIMIT)

                # 资金不足,委托失败
                broker.available_cash = 100
                result = await broker.buy(
                    "0002537.XSHE", 9.43, 10e4, datetime.datetime(2022, 3, 10, 9, 35)
                )

                self.assertEqual(result["status"], EntrustError.NO_CASH)

    async def test_sell(self):
        global bars

        get_limits = "omicron.models.stock.Stock.get_trade_price_limits"
        limits = np.array(
            [(datetime.date(2022, 3, 1), 14, 12.33)],
            dtype=[("frame", "O"), ("high_limit", "<f4"), ("low_limit", "<f4")],
        )

        with mock.patch("backtest.helper.get_app_context", return_value=self.app.ctx):
            self.app.ctx.feed = mock.AsyncMock()
            self.app.ctx.feed.get_bars.return_value = bars_for_test_sell
            self.app.ctx.feed.remove_sell_limit_bars = BaseFeed.remove_sell_limit_bars

            with mock.patch(get_limits, return_value=limits):
                broker = Broker("test", 1000000, 1 / 10000)

                code = "603717.XSHG"
                # 可用余额足够，当前买单足够
                order = Order(
                    "123",
                    code,
                    OrderSide.BUY,
                    1000,
                    9,
                    datetime.datetime(2022, 3, 9, 9, 35),
                    BidType.LIMIT,
                )
                trade = Trade(order, 10, 200, 5.8)
                broker.positions = {code: [trade]}

                result = await broker.sell(
                    code, 12.33, 200, datetime.datetime(2022, 3, 10, 9, 35)
                )
                self.assertEqual(result["status"], EntrustError.SUCCESS)
                data = result["data"]
                self.assertEqual(data.security, code)
                self.assertEqual(data.price, 13.83)
                self.assertEqual(data.shares, 200)
                self.assertAlmostEqual(broker.available_cash, 1002765.72, 2)

                # 可用余额足够，当前买单不足
                order = Order(
                    None,
                    code,
                    OrderSide.BUY,
                    3 * 10e5,
                    9,
                    datetime.datetime(2022, 3, 9, 9, 35),
                    BidType.LIMIT,
                )
                trade = Trade(order, 10, 3 * 10e8, 5.8)
                broker.positions = {code: [trade]}
                broker.available_cash = 0

                result = await broker.sell(
                    code, 12.33, 3 * 10e6, datetime.datetime(2022, 3, 10, 9, 35)
                )
                self.assertEqual(result["status"], EntrustError.SUCCESS)
                self.assertEqual(broker.available_cash, 62907806.59, 2)

                # 可用余额不足，当前买单足够

                # 持股数够，但可用余额不足
