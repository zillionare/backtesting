import datetime
from abc import ABCMeta, abstractmethod
from typing import Dict, List

import jqdatasdk as jq
import numpy as np
from coretypes import Frame, FrameType, SecurityType, bars_dtype
from omicron.extensions.np import math_round
from omicron.models.stock import Stock


class BaseFeed(metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        pass

    async def init(self, *args, **kwargs):
        pass

    async def create_instance(self, interface="zillionare", **kwargs):
        """
        创建实例
        """
        from backtest.feed.zillionarefeed import FileFeed, ZillionareFeed

        if interface == "zillionare":
            feed = ZillionareFeed(**kwargs)
            await feed.init()
        elif interface == "file":
            feed = FileFeed(**kwargs)

    async def get_bars_for_match(self, security: str, start: datetime.datetime) -> list:
        """获取从`start`之后起当天所有的分钟线，用以撮合

        如果feed只支持日线，也是允许的。

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

    async def get_close_price(
        self, secs: List[str], date: datetime.date
    ) -> Dict[str, float]:
        """
        获取证券品种在`date`日期的收盘价

        Args:
            secs : 证券代码列表
            date : 日期

        Returns:
            返回一个字典，key为证券代码，value为收盘价
        """
        raise NotImplementedError

    async def get_trade_price_limits(self, sec: str, date: datetime.date) -> np.ndarray:
        """获取证券的交易价格限制"""
        raise NotImplementedError
