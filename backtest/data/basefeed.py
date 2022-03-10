import datetime
from abc import ABCMeta, abstractmethod
from functools import lru_cache
from typing import List, Tuple

import jqdatasdk as jq
from coretypes import Frame, FrameType, SecurityType, bars_dtype
from omicron.models.stock import Stock

from backtest.data.zillionarefeed import FileFeed, ZillionareFeed


class BaseFeed(ABCMeta):
    def __init__(self):
        pass

    async def init(self):
        pass

    async def create_instance(self, interface="zillionare", **kwargs):
        """
        创建实例
        """
        if interface == "zillionare":
            feed = ZillionareFeed(**kwargs)
            await feed.init()
        elif interface == "file":
            feed = FileFeed(**kwargs)

    @abstractmethod
    async def get_bars(
        self, security: str, ft: FrameType, start: Frame, end: Frame, n: int, fq=True
    ) -> list:
        """获取`security`在`start`到`end`之间的`n`个`ft`的bar

        如果`start`, `end`和`n`都存在且冲突，以[start, end]为准

        Args:
            security : 证券代码
            ft : 帧类型
            start : 起始时间
            end : 结束时间
            n : 帧数量
            fq : 是否返回前复权数据.

        Returns:
            a numpy array which dtype is `bars_dtype`
        """
        pass

    async def get_price(self, security: str) -> bars_dtype:
        """ "
        获取当前实时价格，直接调用jq接口
        """
        fields = ["date", "open", "close", "high", "low", "volume", "amount"]
        return jq.get_bars(
            security, "1m", 1, fields=fields, include_now=True, df=False
        )[0]
