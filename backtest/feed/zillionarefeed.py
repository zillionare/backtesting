import datetime
import logging
from typing import Dict, List, Union

import numpy as np
from coretypes import FrameType
from omicron import array_math_round, math_round, tf
from omicron.extensions.np import fill_nan
from omicron.models.stock import Stock

from backtest.common.errors import EntrustError
from backtest.feed.basefeed import BaseFeed

from . import match_data_dtype

logger = logging.getLogger(__name__)


class ZillionareFeed(BaseFeed):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def init(self, *args, **kwargs):
        pass

    async def get_price_for_match(
        self, security: str, start: datetime.datetime
    ) -> np.ndarray:
        end = datetime.datetime.combine(start.date(), datetime.time(15))
        bars = await Stock.get_bars(security, 240, FrameType.MIN1, end)
        if start.hour * 60 + start.minute <= 571:  # 09:31
            bars[0]["close"] = bars[0]["open"]

        return bars[bars["frame"] >= start][["frame", "close", "volume"]].astype(
            match_data_dtype
        )

    async def get_close_price(self, sec: str, date: datetime.date, fq=False) -> float:
        try:
            bars = await Stock.get_bars(sec, 1, FrameType.DAY, date, fq=fq)
            if len(bars):
                return math_round(bars[-1]["close"].item(), 2)
            else:
                bars = await Stock.get_bars(sec, 500, FrameType.DAY, date, fq=fq)
                return math_round(bars[-1]["close"].item(), 2)
        except Exception as e:
            logger.exception(e)
            logger.warning("get_close_price failed for %s:%s", sec, date)

        return None

    async def batch_get_close_price_in_range(
        self, secs: List[str], frames: List[datetime.date], fq=False
    ) -> Dict[str, np.array]:
        if len(secs) == 0:
            raise ValueError("No securities provided")

        start = frames[0]
        end = frames[-1]
        bars = await Stock.batch_get_bars_in_range(
            secs, FrameType.DAY, start, end, fq=fq
        )

        close_dtype = [("frame", "O"), ("close", "<f4")]
        result = {}

        try:
            for sec, values in bars.items():
                closes = values[["frame", "close"]].astype(close_dtype)
                if len(closes) == 0:
                    # 遇到停牌的情况
                    price = await self.get_close_price(sec, frames[-1], fq=fq)
                    if price is None:
                        result[sec] = None
                    else:
                        result[sec] = np.array(
                            [(f, price) for f in frames], dtype=close_dtype
                        )
                    continue

                closes["close"] = array_math_round(closes["close"], 2)

                # find missed frames, using left fill
                missed = np.setdiff1d(frames, closes["frame"])
                if len(missed):
                    missed = np.array(
                        [(f, np.nan) for f in missed],
                        dtype=close_dtype,
                    )
                    closes = np.concatenate([closes, missed])
                    closes = np.sort(closes, order="frame")
                    closes["close"] = fill_nan(closes["close"])

                result[sec] = closes

            return result
        except Exception:
            logger.warning("get_close_price failed for %s:%s - %s", secs, start, end)
            raise

    async def get_trade_price_limits(self, sec: str, date: datetime.date) -> np.ndarray:
        prices = await Stock.get_trade_price_limits(sec, date, date)

        if len(prices):
            return prices[0]
        else:
            logger.warning("get_trade_price_limits failed for %s:%s", sec, date)
            raise EntrustError(EntrustError.NODATA, security=sec, time=date)

    async def get_dr_factor(
        self, secs: Union[str, List[str]], frames: List[datetime.date]
    ) -> Dict[str, np.ndarray]:
        try:
            data = await Stock.batch_get_bars_in_range(
                secs, FrameType.DAY, frames[0], frames[-1], fq=False
            )

            result = {}

            for sec, bars in data.items():
                factors = bars[["frame", "factor"]].astype(
                    [("frame", "O"), ("factor", "<f4")]
                )

                # find missed frames, using left fill
                missed = np.setdiff1d(frames, bars["frame"])
                if len(missed):
                    missed = np.array(
                        [(f, np.nan) for f in missed],
                        dtype=[("frame", "O"), ("factor", "<f4")],
                    )
                    factors = np.concatenate([factors, missed])
                    factors = np.sort(factors, order="frame")

                if all(np.isnan(factors["factor"])):
                    factors["factor"] = [1.0] * len(factors)
                else:
                    factors["factor"] = fill_nan(factors["factor"])

                result[sec] = factors["factor"] / factors["factor"][0]
            return result
        except Exception as e:
            logger.exception(e)
            logger.warning(
                "get_dr_factor failed for %s:%s ~ %s", secs, frames[0], frames[-1]
            )
            raise
