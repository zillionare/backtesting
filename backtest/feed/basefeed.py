import datetime
from abc import ABCMeta, abstractmethod
from calendar import c
from typing import Dict, List, Union

import numpy as np


class BaseFeed(metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    async def init(self, *args, **kwargs):
        pass

    @classmethod
    async def create_instance(cls, interface="zillionare", **kwargs):
        """
        创建实例
        """
        from backtest.feed.filefeed import FileFeed
        from backtest.feed.zillionarefeed import ZillionareFeed

        if interface == "zillionare":
            feed = ZillionareFeed(**kwargs)
            await feed.init()
            return feed
        elif interface == "file":
            feed = FileFeed(**kwargs)
            return feed
        else:
            raise TypeError(f"{interface} is not supported")

    @abstractmethod
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

    @abstractmethod
    async def get_close_price(
        self, secs: Union[str, List[str]], date: datetime.date
    ) -> Union[float, Dict[str, float]]:
        """
        获取证券品种在`date`日期的收盘价

        Args:
            secs : 证券代码列表
            date : 日期

        Returns:
            如果secs为单支股票，则返回该股收盘价；如果secs为一个列表，则返回一个字典，key为证券代码，value为收盘价
        """
        raise NotImplementedError

    @abstractmethod
    async def get_trade_price_limits(self, sec: str, date: datetime.date) -> np.ndarray:
        """获取证券的交易价格限制"""
        raise NotImplementedError
