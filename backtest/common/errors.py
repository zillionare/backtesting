from enum import IntEnum


class GenericErrCode(IntEnum):
    OK = 0
    UNKNOWN = -1

    def __str__(self):
        return {
            GenericErrCode.OK: "成功",
            GenericErrCode.UNKNOWN: "失败",
        }.get(self)


class Error(Exception):
    """错误基类"""

    def __init__(self, message: str, *args):
        self.message = message
        self.args = args

    def __str__(self):
        return f"{self.message}: {self.args}"


class BadParameterError(Error):
    """参数错误"""

    pass


class NoDataForMatchError(Error):
    """缺少撮合数据"""

    pass


class AccountConflictError(Error):
    """账户冲突"""

    pass
