"""Unit test package for backtest."""
import asyncio
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import uuid
from contextlib import closing
from typing import Union

import aiohttp
import arrow
import cfg4py
import numpy as np
from coretypes import FrameType, bars_dtype
from omicron.models.timeframe import TimeFrame

from backtest.config import get_config_dir
from backtest.feed.filefeed import FileFeed

os.environ[cfg4py.envar] = "DEV"
cfg = cfg4py.init(get_config_dir())
logger = logging.getLogger(__name__)
port = None


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("localhost", 0))
        # s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


async def start_backtest_server(timeout=60):
    global port

    port = find_free_port()

    # account = os.environ["JQ_ACCOUNT"]
    # password = os.environ["JQ_PASSWORD"]
    # jq.auth(account, password)

    process = subprocess.Popen(
        [sys.executable, "-m", "backtest.app", "start", f"--port={port}"],
        env=os.environ,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for i in range(timeout * 10, 0, -1):
        await asyncio.sleep(0.1)
        if process.poll() is not None:
            # already exit, due to finish or fail
            out, err = process.communicate()
            logger.warning(
                "subprocess exited, %s: %s", process.pid, out.decode("utf-8")
            )
            raise subprocess.SubprocessError(err.decode("utf-8"))

        if await is_backtest_server_alive(port):
            # return the process id, the caller should shutdown it later
            logger.info(
                "backtest server(%s) is listen on %s",
                process.pid,
                f"http://localhost:{port}",
            )

            return process

    os.kill(process.pid, signal.SIGINT)
    raise TimeoutError("backtest server is not started.")


async def is_backtest_server_alive(port):
    url = f"http://localhost:{port}/backtest/api/trade/v0.2/status"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                logger.info("url: %s, status: %s", url, resp.status)
                return resp.status == 200
    except Exception:
        return False


async def post(cmd: str, data):
    global port

    url = f"http://localhost:{port}/backtest/api/trade/v0.2/{cmd}"

    logger.info("post %s", url)

    headers = {
        "Authorization": f"Token {cfg.accounts[0]['token']}",
        "Request-ID": uuid.uuid4().hex,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, headers=headers) as resp:
                return await resp.json()
    except Exception:
        return None


async def get(cmd: str, data=None):
    global port

    url = f"http://localhost:{port}/backtest/api/trade/v0.2/{cmd}"
    logger.info("get %s", url)

    headers = {
        "Authorization": f"Token {cfg.accounts[0]['token']}",
        "Request-ID": uuid.uuid4().hex,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, json=data, headers=headers) as resp:
                if resp.content_type == "application/json":
                    return await resp.json()
                elif resp.content_type == "text/plain":
                    return await resp.text()
                else:
                    return await resp.content.read()
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
