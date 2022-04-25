import datetime
import os
import pickle
import unittest

import cfg4py
import omicron
from omicron.models.timeframe import TimeFrame as tf

from backtest.config import get_config_dir
from backtest.feed.basefeed import BaseFeed
from tests import data_populate


class ZillionareFeedTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        cfg4py.init(get_config_dir())
        try:
            await omicron.init()
        except omicron.core.errors.DataNotReadyError:
            tf.service_degrade()

        await data_populate()

        self.feed = await BaseFeed.create_instance()
        return super().setUp()

    async def test_get_bars_for_match(self):
        bars = await self.feed.get_bars_for_match(
            "002537.XSHE", datetime.datetime(2022, 3, 14, 9, 35)
        )

        self.assertEqual(len(bars), 236)
        self.assertEqual(bars[0]["frame"], datetime.datetime(2022, 3, 14, 9, 35))
        self.assertEqual(bars[-1]["frame"], datetime.datetime(2022, 3, 14, 15))

    async def test_get_close_price(self):
        code = "002537.XSHE"
        price = await self.feed.get_close_price([code], datetime.date(2022, 3, 14))
        self.assertAlmostEqual(price[code], 9.56, 2)

    async def test_get_trade_price_limits(self):
        code = "002537.XSHE"
        limits = await self.feed.get_trade_price_limits(
            code, datetime.date(2022, 3, 10)
        )
        self.assertAlmostEqual(9.68, limits[1], 2)
        self.assertAlmostEqual(7.92, limits[2], 2)
