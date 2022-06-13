# noqa
from typing import Optional


class Config(object):
    __access_counter__ = 0

    def __cfg4py_reset_access_counter__(self):
        self.__access_counter__ = 0

    def __getattribute__(self, name):
        obj = object.__getattribute__(self, name)
        if name.startswith("__") and name.endswith("__"):
            return obj

        if callable(obj):
            return obj

        self.__access_counter__ += 1
        return obj

    def __init__(self):
        raise TypeError("Do NOT instantiate this class")

    class metrics:
        risk_free_rate: Optional[float] = None

        annual_days: Optional[int] = None

    class server:
        prefix: Optional[str] = None

    class auth:
        admin: Optional[str] = None

    class feed:
        type: Optional[str] = None

    class postgres:
        enabled: Optional[bool] = None

        dsn: Optional[str] = None

    class redis:
        dsn: Optional[str] = None

    class influxdb:
        url: Optional[str] = None

        token: Optional[str] = None

        org: Optional[str] = None

        bucket_name: Optional[str] = None

        enable_compress: Optional[bool] = None
