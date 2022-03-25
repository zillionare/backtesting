"""Unit test package for backtest."""
import logging
import os
import socket
import uuid
from contextlib import closing
from typing import Union

import arrow
import cfg4py
import numpy as np
from coretypes import FrameType, bars_dtype
from omicron.models.timeframe import TimeFrame
from sanic import Sanic

from backtest.app import application as app
from backtest.config import get_config_dir
from backtest.feed.filefeed import FileFeed
from backtest.web.interfaces import bp

os.environ[cfg4py.envar] = "DEV"
cfg = cfg4py.init(get_config_dir())
logger = logging.getLogger(__name__)


def init_interface_test():
    cfg = cfg4py.init(get_config_dir())

    path = cfg.server.path.rstrip("/")
    bp.url_prefix = path
    app.blueprint(bp)

    return app


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("localhost", 0))
        # s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


async def post(cmd: str, token: str, data):
    url = f"/backtest/api/trade/v0.2/{cmd}"

    headers = {
        "Authorization": f"Token {token}",
        "Request-ID": uuid.uuid4().hex,
    }
    try:
        _, response = await app.asgi_client.post(url, json=data, headers=headers)
        return response.json
    except Exception as e:
        logger.exception(e)
        return None


async def get(cmd: str, token: str, data=None):
    url = f"/backtest/api/trade/v0.2/{cmd}"

    headers = {
        "Authorization": f"Token {token}",
        "Request-ID": uuid.uuid4().hex,
    }
    try:
        _, response = await app.asgi_client.get(url, headers=headers)
        return response.json
    except Exception as e:
        logger.exception(e)
        return None


def data_dir():
    return os.path.join(os.path.dirname(__file__), "data")


def read_csv(fname, start=None, end=None):
    """start, end是行计数，从1开始，以便于与编辑器展示的相一致。
    返回[start, end]之间的行
    """
    path = os.path.join(data_dir(), fname)
    with open(path, "r") as f:
        lines = f.readlines()

    if start is None:
        start = 1  # skip header
    else:
        start -= 1

    if end is None:
        end = len(lines)

    return lines[start:end]


def lines2bars(lines, is_date):
    """将CSV记录转换为Bar对象

    header: date,open,high,low,close,money,volume,factor
    lines: 2022-02-10 10:06:00,16.87,16.89,16.87,16.88,4105065.000000,243200.000000,121.719130

    """
    if isinstance(lines, str):
        lines = [lines]

    def parse_date(x):
        return arrow.get(x).date()

    def parse_naive(x):
        return arrow.get(x).naive

    if is_date:
        convert = parse_date
    else:
        convert = parse_naive

    data = []
    for line in lines:
        fields = line.split(",")
        data.append(
            (
                convert(fields[0]),
                float(fields[1]),
                float(fields[2]),
                float(fields[3]),
                float(fields[4]),
                float(fields[5]),
                float(fields[6]),
                float(fields[7]),
            )
        )

    return np.array(data, dtype=bars_dtype)


def bars_from_csv(
    code: str, ft: Union[str, FrameType], start_line: int = None, end_line: int = None
):
    ft = FrameType(ft)

    fname = f"{code}.{ft.value}.csv"

    if ft in TimeFrame.minute_level_frames:
        is_date = False
    else:
        is_date = True

    return lines2bars(read_csv(fname, start_line, end_line), is_date)


def create_file_feed():
    match_bars_file = os.path.join(data_dir(), "bars_1m.pkl")
    price_limits_file = os.path.join(data_dir(), "limits.pkl")
    return FileFeed(match_bars_file, price_limits_file)
