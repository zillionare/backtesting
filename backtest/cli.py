"""Console script for backtest."""

import logging
import subprocess
import sys
import time

import fire
import psutil
from tqdm import tqdm

logger = logging.getLogger(__name__)


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


def status():
    """检查backtest server是否已经启动"""
    pid = find_backtest_process()
    if pid is None:
        print("backtest server未启动")
        return

    print(f"backtest server正在运行：(pid<{pid}>)")


def stop():
    print("停止backtest server")
    pid = find_backtest_process()
    if pid is None:
        print("backtest server未启动")
        return

    p = psutil.Process(pid)
    p.terminate()
    p.wait()
    print("backtest server停止成功")


def start(port: int = 7080):
    print("启动backtest server")
    if find_backtest_process() is not None:
        print("backtest server已经启动")
        return

    process = subprocess.Popen(
        [sys.executable, "-m", "backtest.app", "start", f"--port={port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    for i in tqdm(range(100)):
        time.sleep(0.1)
        pid = find_backtest_process()
        if pid is not None:
            print(f"backtest server启动成功(pid<{pid}>),耗时{i * 0.1}秒")
            return

        if process.poll() is not None:
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
