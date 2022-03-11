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
from backtest.types import EntrustError
from tests.data import bars_for_test_buy


class BrokerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = Sanic("backtest")
        self.app.ctx = Namespace()

        return await super().asyncSetUp()

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

            with mock.patch(get_limits, return_value=limits):
                broker = Broker("test", 1000000, 1 / 10000)

                # normal order which is total filled
                result = await broker.buy(
                    "002537.XSHE",
                    9.43,
                    10e4,
                    datetime.datetime(2022, 3, 10, 9, 35),
                    request_id="1234abc",
                )
                self.assertEqual(result["status"], EntrustError.SUCCESS)
                self.assertEqual(result["data"].security, "002537.XSHE")
                self.assertAlmostEqual(result["data"].price, 9.1)
                self.assertEqual(result["data"].shares, 10e4)
                self.assertEqual(result["data"].fee, 91)
                self.assertIsNotNone(result["data"].tid)
                self.assertEqual(result["data"].request_id, "1234abc")

                # partial filled
                broker.available_cash = 10e10
                result = await broker.buy(
                    "002537.XSHE",
                    9.43,
                    10e8,  # total available shares: 81840998
                    datetime.datetime(2022, 3, 10, 9, 35),
                    request_id="1234abc",
                )
                self.assertEqual(result["status"], EntrustError.PARTIAL_SUCCESS)
                self.assertEqual(result["data"].security, "002537.XSHE")
                self.assertAlmostEqual(result["data"].price, 9.280280223977718, 2)
                self.assertEqual(result["data"].shares, 81840998)
                self.assertEqual(result["data"].fee, 75950.739525, 2)
                self.assertIsNotNone(result["data"].tid)
                self.assertEqual(result["data"].request_id, "1234abc")

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
