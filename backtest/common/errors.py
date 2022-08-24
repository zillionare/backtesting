"""定义了在backtest中常用的异常类型及异常类型基类
"""
from enum import IntEnum


class BacktestError(BaseException):
    """错误基类"""

    def __init__(self, message: str, *args):
        self.message = message
        self.args = args

    def __str__(self):
        return f"{self.message}: {self.args}"


class BadParameterError(BacktestError):
    """参数错误"""

    pass


class AccountError(BacktestError):
    """账户冲突，或者已冻结"""

    def __init__(self, msg: str = None):
        super().__init__(msg or "账户冲突，或者已冻结")

    def __str__(self):
        return self.message


class EntrustError(BacktestError):
    """交易过程中发生的异常"""

    GENERIC_ERROR = -1
    NO_CASH = -2
    REACH_BUY_LIMIT = -3
    REACH_SELL_LIMIT = -4
    NO_POSITION = -5
    PRICE_NOT_MEET = -6
    NODATA_FOR_MATCH = -7
    NODATA = -8
    TIME_REWIND = -9

    def __init__(self, status_code: int, **kwargs):
        self.status_code = status_code
        self.message = self.__template__().format(**kwargs)

    def __template__(self):
        return {
            EntrustError.GENERIC_ERROR: "委托失败{security}, {time}",
            EntrustError.NO_CASH: "账户{account}资金不足, 需要{required}, 当前{available}",
            EntrustError.REACH_BUY_LIMIT: "不能在涨停板上买入{security}, {time}",
            EntrustError.REACH_SELL_LIMIT: "不能在跌停板上卖出{security}, {time}",
            EntrustError.NO_POSITION: "{security}在{time}期间没有持仓",
            EntrustError.PRICE_NOT_MEET: "{security}现价未达到委托价:{entrust}",
            EntrustError.NODATA_FOR_MATCH: "没有匹配到{security}在{time}的成交数据",
            EntrustError.NODATA: "获取{security}在{time}的行情数据失败，请检查日期是否为交易日，或者当天是否停牌",
            EntrustError.TIME_REWIND: "委托时间必须递增出现。当前{time}, 前一个委托时间{last_trade_time}",
        }.get(self.status_code)
