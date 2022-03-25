import datetime
import os
import unittest
from multiprocessing.sharedctypes import Value
from unittest import mock

from backtest.feed.filefeed import FileFeed
from tests import data_dir


class FileFeedTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        match_path = os.path.join(data_dir(), "bars_1m.pkl")
        limits_path = os.path.join(data_dir(), "limits.pkl")

        self.feed = FileFeed(match_path, limits_path)
        await self.feed.init()

        return super().setUp()

    async def test_get_bars_for_match(self):
        start = datetime.datetime(2022, 3, 10, 14, 52)
        code = "002537.XSHE"
        bars = await self.feed.get_bars_for_match(code, start)
        self.assertEqual(len(bars), 9)
        self.assertEqual(bars[0]["frame"], start)
        self.assertEqual(bars[-1]["frame"], datetime.datetime(2022, 3, 10, 15))

    async def test_get_close_price(self):
        codes = ["002537.XSHE", "603717.XSHG"]
        prices = await self.feed.get_close_price(codes, datetime.date(2022, 3, 10))
        print(prices)

        self.assertAlmostEqual(prices["002537.XSHE"], 9.68, 2)
        self.assertAlmostEqual(prices["603717.XSHG"], 12.33, 2)

    async def test_get_trade_price_limits(self):
        code = "002537.XSHE"
        dt, high, low = await self.feed.get_trade_price_limits(
            code, datetime.date(2022, 3, 10)
        )

        self.assertAlmostEqual(high, 9.68, 2)
        self.assertAlmostEqual(low, 7.92, 2)

        self.feed.price_limits = {}
        with self.assertRaises(ValueError):
            await self.feed.get_trade_price_limits(code, datetime.date(2022, 3, 10))
