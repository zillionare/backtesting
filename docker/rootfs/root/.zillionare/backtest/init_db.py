"""Unit test package for backtest."""
import asyncio
import logging
import os
import pickle

import cfg4py
import omicron
import pandas as pd
from coretypes import FrameType
from omicron.dal.influx.influxclient import InfluxClient
from omicron.models.stock import Stock

from backtest.config import get_config_dir

print("server role is configured as:", os.environ[cfg4py.envar])
print("config dir is:", get_config_dir())

cfg = cfg4py.init(get_config_dir())


def data_dir():
    return "/root/.zillionare/backtest/data"


async def data_populate():
    url, token, bucket, org = (
        cfg.influxdb.url,
        cfg.influxdb.token,
        cfg.influxdb.bucket,
        cfg.influxdb.org,
    )
    client = InfluxClient(url, token, bucket, org)

    # fill in influxdb
    await client.drop_measurement("stock_bars_1d")
    await client.drop_measurement("stock_bars_1m")

    for ft in (FrameType.MIN1, FrameType.DAY):
        file = os.path.join(data_dir(), f"bars_{ft.value}.pkl")
        assert os.path.exists(file)
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

    await omicron.init()
    await data_populate()


asyncio.run(main())
