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

import aiohttp
import cfg4py
import jqdatasdk as jq

from backtest.config import get_config_dir

cfg = cfg4py.init(get_config_dir())
logger = logging.getLogger(__name__)
port = None


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("localhost", 0))
        # s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = s.getsockname()[1]
        return port


async def start_backtest(timeout=60):
    global port

    port = find_free_port()

    # account = os.environ["JQ_ACCOUNT"]
    # password = os.environ["JQ_PASSWORD"]
    # jq.auth(account, password)

    process = subprocess.Popen(
        [sys.executable, "-m", "backtest.backtest", "start"],
        env=os.environ,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for i in range(timeout, 0, -1):
        await asyncio.sleep(1)
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
    url = f"http://localhost:{port}/api/trade/v0.1/status"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return resp.status == 200
    except Exception:
        return False


async def post(cmd: str, data):
    global port

    url = f"http://localhost:{port}/api/trade/v0.1/{cmd}"

    headers = {
        "Authorization": f"Token {cfg.account.token}",
        "Request-ID": uuid.uuid4().hex,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, headers=headers) as resp:
                return await resp.json()
    except Exception:
        return None
