import datetime
from abc import ABCMeta, abstractmethod
from typing import Dict, List, Tuple, Union

import numpy as np
import pandas as pd


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
        self, secs: List[str], start: datetime.date, end: datetime.date, fq=False
    ) -> pd.DataFrame:
        """获取多个证券在[start, end]期间的收盘价。

        如果股票在[start,end]期间停牌，返回值将使用`ffill`填充。如果在`start`当天停牌，则将调用`get_close_price`取`start`前一期的收盘价。

        返回值示例:

        |          日期       | 000001.XSHE | 600000.XSHG |
        |--------------------|-------------|-------------|
        |         2022-03-01 |        9.51 |        4.56 |
        |         2022-03-02 |          10 |  5          |


        Args:
            secs: 证券代码列表
            start: 起始日期
            end: 截止日期
            fq: 是否复权。

        Returns:
            a dataframe which frames is index (sorted) and each sec as columns
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
        self,
        secs: Union[str, List[str]],
        frames: List[datetime.date],
        normalized: bool = True,
    ) -> pd.DataFrame:
        """股票在[start,end]间的每天的复权因子，使用start日进行归一化处理

        注意实现者必须保证，复权因子的长度与日期的长度相同且完全对齐。如果遇到停牌的情况，应该进行相应的填充。

        Args:
            secs: 证券列表。如果传入str，则将会转换为列表
            frames: 待取日期。注意很多时候，可能需要传入起始日期之前的那个日期，以便对复权因子进行归一化，而不丢失信息。
            normalized: 传回的复权因子是否归一化到frames[0]
        Returns:
            返回复权因子DataFrame，其中secs为列，frames为index，每一个cell为该股该天的复权因子
        """
        raise NotImplementedError
