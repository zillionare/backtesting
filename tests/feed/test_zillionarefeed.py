import datetime
import os
import pickle
import unittest
from unittest import mock

import cfg4py
import numpy as np
import omicron
from coretypes import FrameType
from omicron.models.stock import Stock
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

    async def test_get_price_for_match(self):
        bars = await self.feed.get_price_for_match(
            "002537.XSHE", datetime.datetime(2022, 3, 14, 9, 35)
        )

        self.assertEqual(len(bars), 236)
        self.assertEqual(bars[0]["frame"], datetime.datetime(2022, 3, 14, 9, 35))
        self.assertEqual(bars[-1]["frame"], datetime.datetime(2022, 3, 14, 15))

        bars = await self.feed.get_price_for_match(
            "002537.XSHE", datetime.datetime(2022, 3, 14, 9, 31)
        )
        self.assertAlmostEqual(bars[0]["price"], 9.83, 2)
        self.assertAlmostEqual(bars[0]["volume"], 12639700, 0)

    async def test_get_close_price(self):
        code = "002537.XSHE"
        start = datetime.date(2022, 3, 11)
        end = datetime.date(2022, 3, 14)
        price = await self.feed.get_close_price(code, end)
        np.testing.assert_array_almost_equal(price, 9.56)

    async def test_batch_get_close_price_in_range(self):
        # test padding
        start = datetime.date(2022, 3, 9)
        end = datetime.date(2022, 3, 14)
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_bars_in_range",
            return_value={
                "603717.XSHG": np.array(
                    [
                        (start, 13.7),
                        (end, 10.1),
                    ],
                    dtype=[("frame", "O"), ("close", "<f4")],
                )
            },
        ):
            frames = [tf.int2date(d) for d in tf.get_frames(start, end, FrameType.DAY)]
            price = await self.feed.batch_get_close_price_in_range(
                ["603717.XSHG"], frames
            )
            np.testing.assert_array_almost_equal(
                price["603717.XSHG"]["close"], [13.7, 13.7, 13.7, 10.1]
            )

    async def test_get_trade_price_limits(self):
        code = "002537.XSHE"
        limits = await self.feed.get_trade_price_limits(
            code, datetime.date(2022, 3, 10)
        )
        self.assertAlmostEqual(9.68, limits[1], 2)
        self.assertAlmostEqual(7.92, limits[2], 2)

        data = {
            "002537.XSHE": np.array(
                [
                    (datetime.date(2022, 3, 7), 10, 0.95),
                    (datetime.date(2022, 3, 8), 9, 1.1),
                    (datetime.date(2022, 3, 14), 8, 1.2),
                ],
                dtype=[("frame", "O"), ("close", "f8"), ("factor", "f8")],
            )
        }

        start = datetime.date(2022, 3, 7)
        end = datetime.date(2022, 3, 14)
        frames = [tf.int2date(d) for d in tf.get_frames(start, end, FrameType.DAY)]
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_bars_in_range", return_value=data
        ):
            dr = await self.feed.get_dr_factor(["002537.XSHE"], frames)

            dr = dr.get("002537.XSHE")
            exp = [1.0, 1.16, 1.16, 1.16, 1.16, 1.26]
            np.testing.assert_array_almost_equal(dr, exp, decimal=2)
