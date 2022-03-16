import datetime
import os
import unittest

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
        codes = ["002537.XSHE", "063717.XSHG"]
        prices = await self.feed.get_close_price(codes, datetime.date(2022, 3, 10))
        print(prices)

    async def test_get_trade_price_limits(self):
        code = "002537.XSHE"
        limits = await self.feed.get_trade_price_limits(
            code, datetime.date(2022, 3, 10)
        )
        print(limits)
