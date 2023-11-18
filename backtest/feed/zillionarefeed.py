import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from coretypes import FrameType
from coretypes.errors.trade import NoData
from omicron import tf
from omicron.core.backtestlog import BacktestLogger
from omicron.extensions import array_math_round, math_round
from omicron.models.stock import Stock

from backtest.feed import match_data_dtype
from backtest.feed.basefeed import BaseFeed

logger = BacktestLogger.getLogger(__name__)


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

    async def get_close_price(
        self, sec: str, date: datetime.date, fq=False
    ) -> Optional[float]:
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
        self, secs: List[str], start: datetime.date, end: datetime.date, fq=False
    ) -> pd.DataFrame:
        if len(secs) == 0:
            raise ValueError("No securities provided")

        frames = [tf.int2date(f) for f in tf.get_frames(start, end, FrameType.DAY)]
        sec_dfs = [pd.DataFrame([], index=frames)]
        try:
            async for sec, values in Stock.batch_get_day_level_bars_in_range(
                secs, FrameType.DAY, start, end, fq=fq
            ):
                if len(values) > 0:  # 停牌的情况
                    close = array_math_round(values["close"], 2)  # type: ignore
                else:
                    close = []
                df = pd.DataFrame(
                    data=close,
                    columns=[sec],
                    index=[v.item().date() for v in values["frame"]],
                )  # type: ignore

                if len(df) == 0 or df.index[0] > start:  # type: ignore
                    close = await self.get_close_price(sec, start)
                    df.loc[start, sec] = close
                sec_dfs.append(df)

            df = pd.concat(sec_dfs, axis=1)
            df.sort_index(inplace=True)
            df.fillna(method="ffill", inplace=True)
            return df
        except Exception:
            logger.warning("get_close_price failed for %s:%s - %s", secs, start, end)
            raise

    async def get_trade_price_limits(self, sec: str, date: datetime.date) -> np.ndarray:
        prices = await Stock.get_trade_price_limits(sec, date, date)

        if len(prices):
            return prices[0]
        else:
            logger.warning("get_trade_price_limits failed for %s:%s", sec, date)
            raise NoData(sec, date)

    async def get_dr_factor(
        self,
        secs: Union[str, List[str]],
        frames: List[datetime.date],
        normalized: bool = True,
    ) -> pd.DataFrame:
        if isinstance(secs, str):
            secs = [secs]
        try:
            dfs = [pd.DataFrame([], index=frames)]
            async for sec, bars in Stock.batch_get_day_level_bars_in_range(
                secs, FrameType.DAY, frames[0], frames[-1], fq=False
            ):
                df = pd.DataFrame(
                    data=bars["factor"],  # type: ignore
                    columns=[sec],
                    index=[v.item().date() for v in bars["frame"]],
                )  # type: ignore
                if normalized:  # fixme: 长时间停牌+复权会导致此处出错，因为iloc[0]可能停牌
                    df[sec] = df[sec] / df.iloc[0][sec]
                dfs.append(df)

            df = pd.concat([pd.DataFrame([], index=frames), *dfs], axis=1)
            df.sort_index(inplace=True)
            # issue 13: 停牌时factor假设为1
            df.iloc[0].fillna(1.0, inplace=True)
            df.ffill(inplace=True)

            return df
        except Exception as e:
            logger.exception(e)
            logger.warning(
                "get_dr_factor failed for %s:%s ~ %s", secs, frames[0], frames[-1]
            )
            raise
