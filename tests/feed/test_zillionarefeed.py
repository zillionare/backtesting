import datetime
import unittest
from unittest import mock

import arrow
import cfg4py
import numpy as np
import omicron
from coretypes import FrameType
from omicron.models.timeframe import TimeFrame as tf
from pyemit import emit
from sanic import Sanic

from backtest.config import get_config_dir
from backtest.feed.basefeed import BaseFeed
from tests import data_populate

hljh = "002537.XSHE"


def disable_listeners():
    """these listener will cause omicron to be closed"""
    app = Sanic.get_app("backtest")
    app.ctx.before_server_start = []
    app.ctx.after_server_start = []


class ZillionareFeedTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        cfg4py.init(get_config_dir())
        disable_listeners()
        try:
            await omicron.init()
        except omicron.core.errors.DataNotReadyError:
            tf.service_degrade()

        await data_populate()

        self.feed = await BaseFeed.create_instance()
        return super().setUp()

    async def asyncTearDown(self) -> None:
        await omicron.close()
        await emit.stop()

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

        # first get_bars returns None
        with mock.patch(
            "omicron.models.stock.Stock.get_bars",
            side_effect=[[], np.array([1000], dtype=[("close", "f4")])],
        ):
            price = await self.feed.get_close_price(code, end)
            self.assertEqual(price, 1000)

        # test error handling
        with mock.patch(
            "omicron.models.stock.Stock.get_bars", side_effect=Exception("error")
        ):
            await self.feed.get_close_price(code, end)

    @mock.patch("arrow.now", return_value=arrow.get("2022-03-14 15:00:00"))
    async def test_batch_get_close_price_in_range(self, mocked_now):
        # test padding middle
        start = datetime.date(2022, 3, 9)
        end = datetime.date(2022, 3, 14)
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range",
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                "603717.XSHG": np.array(
                    [
                        (start, 13.7),
                        (end, 10.1),
                    ],
                    dtype=[("frame", "datetime64[s]"), ("close", "<f4")],
                )
            }.items()

            price = await self.feed.batch_get_close_price_in_range(
                ["603717.XSHG"], start, end
            )
            np.testing.assert_array_almost_equal(
                price["603717.XSHG"], [13.7, 13.7, 13.7, 10.1]
            )

        # 从头起停牌，返回停牌前一天价格13.7
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = {
                "603717.XSHG": np.array(
                    [], dtype=[("frame", "datetime64[s]"), ("close", "<f4")]
                )
            }.items()

            price = await self.feed.batch_get_close_price_in_range(
                ["603717.XSHG"], start, end
            )

            np.testing.assert_array_almost_equal(price["603717.XSHG"], [13.7] * 4)

            # more than one sec, 其中一个从头停牌，返回停牌前一天数据
            with mock.patch(
                "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
            ) as mocked:
                mocked.return_value.__aiter__.return_value = {
                    "603717.XSHG": np.array(
                        [
                            (datetime.date(2022, 3, 10), 9.3),
                        ],
                        dtype=[("frame", "datetime64[s]"), ("close", "<f4")],
                    ),
                    "002537.XSHE": np.array(
                        [
                            (datetime.date(2022, 3, 9), 10.20),
                            (datetime.date(2022, 3, 11), 10.21),
                            (datetime.date(2022, 3, 14), 10.22),
                        ],
                        dtype=[("frame", "datetime64[s]"), ("close", "<f4")],
                    ),
                }.items()

                price = await self.feed.batch_get_close_price_in_range(
                    ["603717.XSHG", "002537.XSHE"], start, end
                )
                np.testing.assert_array_almost_equal(
                    price["603717.XSHG"], [13.7, 9.3, 9.3, 9.3]
                )
                np.testing.assert_array_almost_equal(
                    price["002537.XSHE"], [10.20, 10.20, 10.21, 10.22]
                )
                frames = [
                    tf.int2date(f) for f in tf.get_frames(start, end, FrameType.DAY)
                ]
                np.testing.assert_array_equal(price.index, frames)

    async def test_get_trade_price_limits(self):
        """also test get_dr_factor"""
        code = "002537.XSHE"
        limits = await self.feed.get_trade_price_limits(
            code, datetime.date(2022, 3, 10)
        )
        self.assertAlmostEqual(9.68, limits[1], 2)
        self.assertAlmostEqual(7.92, limits[2], 2)

    async def test_get_dr_factor(self):
        start = datetime.date(2022, 3, 7)
        end = datetime.date(2022, 3, 14)
        frames = [tf.int2date(d) for d in tf.get_frames(start, end, FrameType.DAY)]

        code = "002537.XSHE"
        dr = await self.feed.get_dr_factor([code], frames)
        self.assertEqual(len(dr), 6)
        self.assertEqual(len(dr[code]), 6)
        np.testing.assert_array_almost_equal(dr[code], [1.0] * 6)

        # https://github.com/zillionare/trader-client/issues/13
        code = "000001.XSHE"
        data = {
            code: np.array(
                [
                    (datetime.datetime(2022, 3, 7), np.nan, np.nan),
                    (datetime.datetime(2022, 3, 8), np.nan, np.nan),
                    (datetime.datetime(2022, 3, 9), np.nan, np.nan),
                    (datetime.datetime(2022, 3, 10), np.nan, np.nan),
                ],
                dtype=[("frame", "datetime64[s]"), ("close", "f8"), ("factor", "f8")],
            )
        }

        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range",
        ) as mocked:
            mocked.return_value.__aiter__.return_value = data.items()

            frames = [f.item().date() for f in data[code]["frame"]]
            dr = await self.feed.get_dr_factor([code], frames)
            np.testing.assert_array_equal(dr[code], [1.0] * 4)

        data = {
            "002537.XSHE": np.array(
                [
                    (datetime.datetime(2022, 3, 7), 10, 0.95),
                    (datetime.datetime(2022, 3, 8), 9, 1.1),
                    (datetime.datetime(2022, 3, 14), 8, 1.2),
                ],
                dtype=[("frame", "datetime64[s]"), ("close", "f8"), ("factor", "f8")],
            )
        }

        start = datetime.date(2022, 3, 7)
        end = datetime.date(2022, 3, 14)
        frames = [tf.int2date(d) for d in tf.get_frames(start, end, FrameType.DAY)]
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = data.items()
            dr = await self.feed.get_dr_factor(["002537.XSHE"], frames)

            exp = [1.0, 1.16, 1.16, 1.16, 1.16, 1.26]
            np.testing.assert_array_almost_equal(dr[hljh], exp, decimal=2)

        start = datetime.date(2022, 3, 4)
        end = datetime.date(2022, 3, 14)
        frames = [tf.int2date(d) for d in tf.get_frames(start, end, FrameType.DAY)]
        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.return_value = data.items()
            dr = await self.feed.get_dr_factor(["002537.XSHE"], frames)

            exp = [1.0, 1.0, 1.16, 1.16, 1.16, 1.16, 1.26]
            np.testing.assert_array_almost_equal(dr[hljh], exp, decimal=2)

        with mock.patch(
            "omicron.models.stock.Stock.batch_get_day_level_bars_in_range"
        ) as mocked:
            mocked.return_value.__aiter__.side_effect = Exception
            with self.assertRaises(Exception):
                dr = await self.feed.get_dr_factor([code], data[code]["frame"])
