import datetime
from abc import ABCMeta, abstractmethod
from calendar import c
from typing import Dict, List, Tuple, Union

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
        创建feed实例。当前仅支持zillionare接口。该接口要求使用[zillionare-omicron](https://zillionare.github.io/omicron/)来提供数据。
        """
        from backtest.feed.zillionarefeed import ZillionareFeed

        if interface == "zillionare":
            feed = ZillionareFeed(**kwargs)
            await feed.init()
            return feed
        else:
            raise TypeError(f"{interface} is not supported")

    @abstractmethod
    async def get_price_for_match(
        self, security: str, start: datetime.datetime
    ) -> np.ndarray:
        """获取从`start`之后起当天所有的行情数据，用以撮合

        这里没有要求指定行情数据的时间帧类型，理论上无论从tick级到日线级，backtest都能支持。返回的数据至少要包括`frame`、`price`、`volume`三列。

        Args:
            security : 证券代码
            start : 起始时间
        Returns:
            a numpy array which dtype is `match_data_dtype`
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
    async def get_trade_price_limits(self, sec: str, date: datetime.date) -> Tuple:
        """获取证券的交易价格限制

        获取证券`sec`在`date`日期的交易价格限制。

        Args:
            sec : 证券代码
            date : 日期

        Returns:
            交易价格限制，元组，(日期，涨停价，跌停价)
        """
        raise NotImplementedError
