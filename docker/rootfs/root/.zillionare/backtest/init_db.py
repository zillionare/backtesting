"""Unit test package for backtest."""
import asyncio
import os
import pickle

import cfg4py
import omicron
import pandas as pd
from coretypes import FrameType
from omicron.core.backtestlog import BacktestLogger
from omicron.dal.influx.influxclient import InfluxClient
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame

from backtest.config import get_config_dir

logger = BacktestLogger.getLogger(__name__)

logger.info("server role is %s", os.getenv(cfg4py.envar))
os.environ[cfg4py.envar] = "TEST"
cfg = cfg4py.init(get_config_dir())


def data_dir():
    return "/root/.zillionare/backtest/data"


async def data_populate():
    url, token, bucket, org = (
        cfg.influxdb.url,
        cfg.influxdb.token,
        cfg.influxdb.bucket_name,
        cfg.influxdb.org,
    )
    logger.info("influxdb: %s, %s, %s, %s", url, token, bucket, org)
    client = InfluxClient(url, token, bucket, org)

    # fill in influxdb
    await client.drop_measurement("stock_bars_1d")
    await client.drop_measurement("stock_bars_1m")

    for ft in (FrameType.MIN1, FrameType.DAY):
        file = os.path.join(data_dir(), f"bars_{ft.value}.pkl")
        if not os.path.exists(file):
            print(f"{file} not found", file)

        with open(file, "rb") as f:
            bars = pickle.load(f)
            await Stock.persist_bars(ft, bars)

    df = pd.read_csv(
        os.path.join(data_dir(), "limits.csv"), sep="\t", parse_dates=["time"]
    )
    limits = df.to_records(index=False)
    limits.dtype.names = ["frame", "code", "high_limit", "low_limit"]
    await Stock.save_trade_price_limits(limits, False)


async def main():
    print("influxdb is configured at", cfg.influxdb.url)

    try:
        await omicron.init()
    except Exception:
        TimeFrame.service_degrade()

    try:
        await data_populate()
    except Exception as e:
        logger.exception(e)


asyncio.run(main())
