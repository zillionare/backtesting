"""Main module."""
import asyncio
import logging

import cfg4py
import omicron
from omicron.models.timeframe import TimeFrame
from sanic import Sanic

from backtest.config import get_config_dir
from backtest.feed.basefeed import BaseFeed
from backtest.handlers import bp

app = Sanic("backtest")
logger = logging.getLogger(__name__)


async def application_init(app, *args):
    logger.info("init backtest server")
    try:
        await omicron.init()
    except Exception:
        TimeFrame.service_degrade()

    feed = await BaseFeed.create_instance(interface="zillionare")
    app.ctx.feed = feed


def start(port: int = 7080):
    cfg4py.init(get_config_dir())

    app.blueprint(bp)
    app.register_listener(application_init, "before_server_start")
    app.run(host="0.0.0.0", port=port, register_sys_signals=True)
    logger.info("backtest server stopped")


if __name__ == "__main__":
    start()
