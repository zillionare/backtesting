import datetime
from abc import ABCMeta, abstractmethod
from typing import List

import jqdatasdk as jq
import numpy as np
from coretypes import Frame, FrameType, SecurityType, bars_dtype
from omicron.extensions.np import math_round
from omicron.models.stock import Stock


class BaseFeed(ABCMeta):
    def __init__(self):
        pass

    async def init(self):
        pass

    async def create_instance(self, interface="zillionare", **kwargs):
        """
        创建实例
        """
        from backtest.data.zillionarefeed import FileFeed, ZillionareFeed

        if interface == "zillionare":
            feed = ZillionareFeed(**kwargs)
            await feed.init()
        elif interface == "file":
            feed = FileFeed(**kwargs)

    @abstractmethod
    async def get_minute_bars(self, security: str, start: Frame) -> list:
        """获取从`start`之后起当天所有的分钟线，用以撮合

        Args:
            security : 证券代码
            start : 起始时间
        Returns:
            a numpy array which dtype is `bars_dtype`
        """
        raise NotImplementedError

    async def get_price(self, security: str) -> bars_dtype:
        """ "
        获取当前实时价格，直接调用jq接口
        """
        fields = ["date", "open", "close", "high", "low", "volume", "amount"]
        return jq.get_bars(
            security, "1m", 1, fields=fields, include_now=True, df=False
        )[0]

    async def get_close_price(self, secs: List[str], date: datetime.date) -> float:
        """
        获取证券品种在`date`日期的收盘价
        """
        raise NotImplementedError

    @classmethod
    def remove_buy_limit_bars(cls, bars: np.ndarray, price: float) -> np.ndarray:
        """
        去掉已达到涨停时的分钟线
        """
        close = math_round(bars["close"], 2)
        price = math_round(price, 2)
        return bars[~(close >= price)]

    @classmethod
    def remove_sell_limit_bars(cls, bars: np.ndarray, price: float) -> np.ndarray:
        """去掉已跌停的分钟线"""
        close = math_round(bars["close"], 2)
        price = math_round(price, 2)
        return bars[~(close <= price)]
