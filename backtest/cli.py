"""Console script for backtest."""

import logging
import os
import signal
import subprocess
import sys
import time

import cfg4py
import fire
import psutil
import requests
from tqdm import tqdm

from backtest.config import get_config_dir

logger = logging.getLogger(__name__)

cfg = cfg4py.init(get_config_dir())


def help():
    print("backtest")
    print("=" * len("backtest"))
    print("backtest framework")


def find_backtest_process():
    """查找backtest进程

    backtest进程在ps -aux中显示应该包含 backtest.app --port=&ltport&gt信息
    """
    for p in psutil.process_iter():
        cmd = " ".join(p.cmdline())
        if "backtest.app start" in cmd:
            return p.pid

    return None


def is_running(port, path):
    url = f"http://localhost:{port}/{path}/status"

    try:
        r = requests.get(url)
        return r.status_code == 200
    except Exception:
        return False


def status():
    """检查backtest server是否已经启动"""
    pid = find_backtest_process()
    if pid is None:
        print("backtest server未启动")
        return

    port = cfg.server.port
    path = cfg.server.path.strip("/")

    if is_running(port, path):
        print("\n=== backtest server is RUNNING ===")
        print("pid:", pid)
        print("port:", port)
        print("path:", path)
        print("\n")
    else:
        print("=== backtest server is DEAD ===")
        os.kill(pid, signal.SIGKILL)


def stop():
    print("停止backtest server...")
    pid = find_backtest_process()
    if pid is None:
        print("backtest server未启动")
        return

    p = psutil.Process(pid)
    p.terminate()
    p.wait()
    print("backtest server已停止服务")


def start(port: int = None):
    path = cfg.server.path.strip("/")
    port = port or cfg.server.port

    if is_running(port, path):
        status()
        return

    print("启动backtest server")

    process = subprocess.Popen(
        [sys.executable, "-m", "backtest.app", "start", f"--port={port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    for i in tqdm(range(100)):
        time.sleep(0.1)
        if is_running(port, path):
            status()
            return

        if process.poll() is not None:  # pragma: no cover
            # already exit, due to finish or fail
            out, err = process.communicate()
            logger.warning(
                "subprocess exited, %s: %s", process.pid, out.decode("utf-8")
            )
            raise subprocess.SubprocessError(err.decode("utf-8"))
    else:
        print("backtest server启动超时或者失败。")


def main():
    fire.Fire(
        {
            "help": help,
            "start": start,
            "stop": stop,
            "status": status,
        }
    )


if __name__ == "__main__":
    main()  # pragma: no cover
