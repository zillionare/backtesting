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
    async def get_close_price(self, sec: str, date: datetime.date, fq=False) -> float:
        """
        获取证券品种在`date`日期的收盘价

        Args:
            sec: 证券代码
            date: 日期
            fq: 是否进行前复权

        Returns:
            `sec`在`date`日的收盘价
        """
        raise NotImplementedError

    @abstractmethod
    async def batch_get_close_price_in_range(
        self, secs: List[str], frames: List[datetime.date], fq=False
    ) -> Dict[str, np.array]:
        """获取多个证券在多个日期的收盘价

        Args:
            secs: 证券代码列表
            frames: 日期列表, 日期必须是有序且连续
            fq: 是否复权。

        Raises:
            NotImplementedError:

        Returns:
            a dict which key is `sec` and value is a numpy array which dtype is `[("frame", "O"), ("close", "f4")]`
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

    @abstractmethod
    async def get_dr_factor(
        self, secs: Union[str, List[str]], frames: List[datetime.date]
    ) -> Dict[str, np.array]:
        """股票在[start,end]间的每天的复权因子，使用start日进行归一化处理

        注意实现者必须保证，复权因子的长度与日期的长度相同且完全对齐。如果遇到停牌的情况，应该进行相应的填充。

        Args:
            secs: 股票代码
            frames: 日期列表

        Returns:
            返回一个dict
        """
        raise NotImplementedError
