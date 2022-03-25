import datetime
from typing import Dict, List, Union

import cfg4py
import numpy as np
import omicron
from coretypes import FrameType
from omicron.models.stock import Stock

from backtest.feed.basefeed import BaseFeed


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
        bars = await Stock.batch_get_bars(secs, 1, FrameType.DAY, date)

        if isinstance(secs, str):
            return bars[secs]["close"][0]
        else:
            return {sec: bars[sec]["close"][0] for sec in secs}

    async def get_trade_price_limits(self, sec: str, date: datetime.date) -> np.ndarray:
        prices = await Stock.get_trade_price_limits(sec, date, date)

        if len(prices):
            return prices[0]
