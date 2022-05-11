import datetime
import logging
from typing import Dict, List, Union

import numpy as np
from coretypes import FrameType
from omicron import math_round
from omicron.models.stock import Stock

from backtest.feed.basefeed import BaseFeed

logger = logging.getLogger(__name__)


class ZillionareFeed(BaseFeed):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def init(self, *args, **kwargs):
        pass

    async def get_bars_for_match(self, security: str, start: datetime.datetime) -> list:
        end = datetime.datetime.combine(start.date(), datetime.time(15))
        bars = await Stock.get_bars(security, 240, FrameType.MIN1, end)
        return bars[bars["frame"] >= start]

    async def get_close_price(
        self, secs: Union[str, List[str]], date: datetime.date
    ) -> Union[float, Dict[str, float]]:
        if isinstance(secs, str):
            secs = [secs]

        if len(secs) == 0:
            raise ValueError("No securities provided")

        bars = await Stock.batch_get_bars(secs, 1, FrameType.DAY, date)

        try:
            if isinstance(secs, str):
                return bars[secs]["close"][0]
            else:
                return {sec: math_round(bars[sec]["close"][0], 2) for sec in secs}
        except IndexError:
            logger.warning("get_close_price failed for %s:%s", secs, date)
            raise

    async def get_trade_price_limits(self, sec: str, date: datetime.date) -> np.ndarray:
        prices = await Stock.get_trade_price_limits(sec, date, date)

        if len(prices):
            return prices[0]
