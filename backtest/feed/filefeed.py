import datetime
import logging
import os
import pickle
from typing import Dict, List, Tuple, Union

from backtest.feed.basefeed import BaseFeed

logger = logging.getLogger(__name__)


class FileFeed(BaseFeed):
    def __init__(
        self,
        bars_for_match_path: str,
        price_limits_path: str,
        is_day_level: bool = False,
    ):
        """

        数据文件必须为pkl，撮合数据文件应该是一个dict，key为security，value为dtype为[('frame', 'O'), ('close', '<f8'), ('volume', '<f8')]的numpy array, 如果value中包含其它字段，也是允许的。为提供更好的撮合，建议数据的时间粒度为分钟级别，但也允许最高使用日线。当指定为分钟级别（可以是1分钟或者多分钟）时，应该包括15:00收盘的那一帧。如果为日线，frame的数据类型应该为datetime.date。

        涨停跌数据文件应该是一个dict，key为security，value为dtype为[('frame', 'O'), ('high_limit', '<f8'), ('low_limit', '<f8')]的numpy array, 其中`frame`类型为datetime.date。如果value中包含其它字段，也是允许的。

        Args:
            bars_for_match_path : 撮合分钟线数据路径
            price_limits_path : 涨跌停价格数据路径
            is_day_level : 撮合数据是否为日线？默认False。
        """
        self.bars_for_match_path = bars_for_match_path
        self.price_limits_path = price_limits_path

        self.is_day_level = is_day_level
        super().__init__()

    async def init(self, *args, **kwargs):
        if not os.path.exists(self.bars_for_match_path) or not os.path.exists(
            self.price_limits_path
        ):
            raise FileNotFoundError(
                f"{self.bars_for_match_path} or {self.price_limits_path} not found"
            )

        with open(self.bars_for_match_path, "rb") as f:
            self.bars_for_match = pickle.load(f)

        with open(self.price_limits_path, "rb") as f:
            self.price_limits = pickle.load(f)

    async def get_bars_for_match(self, security: str, start: datetime.datetime) -> list:
        bars = self.bars_for_match.get(security)
        if bars is None:
            logger.warning("%s not found in bars_for_match", security)

        if self.is_day_level:
            start = end_of_day = start.date()
        else:
            end_of_day = datetime.datetime.combine(start, datetime.time(15))

        return bars[(bars["frame"] >= start) & (bars["frame"] <= end_of_day)]

    async def get_close_price(
        self, secs: Union[str, List[str]], date: datetime.date
    ) -> Union[float, Dict[str, float]]:
        if self.is_day_level:
            end_of_day = date
        else:
            end_of_day = datetime.datetime.combine(date, datetime.time(15))

        result = {}

        for sec in secs:
            bars = self.bars_for_match.get(sec)
            if bars is None:
                logger.warning("no bars data for match: %s", sec)
                continue

            closes = bars[bars["frame"] == end_of_day]["close"]
            if len(closes) == 0:
                logger.warning("sec %s has no frame: %s", sec, end_of_day)
                continue

            result[sec] = closes[0]

        return result

    async def get_trade_price_limits(
        self, sec: str, date: datetime.date
    ) -> Tuple[datetime.date, float, float]:
        if getattr(date, "date", None) is not None:
            date = date.date()

        bars = self.price_limits.get(sec)
        if bars is None:
            logger.warning("no price limits data: %s", sec)
            raise ValueError(f"no price limits data: {sec}")

        else:
            assert isinstance(date, datetime.date)
            bar = bars[bars["frame"] == date]
            return bar[0]
