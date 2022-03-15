"""Main module."""
import logging

import cfg4py
import fire
import omicron
from omicron.models.timeframe import TimeFrame
from sanic import Sanic

from backtest.api import bp
from backtest.config import get_config_dir
from backtest.feed.basefeed import BaseFeed

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
    logger.info("start backtest server at port %s", port)
    cfg4py.init(get_config_dir())

    app.blueprint(bp)
    app.register_listener(application_init, "before_server_start")
    app.run(host="0.0.0.0", port=port, register_sys_signals=True)
    logger.info("backtest server stopped")


if __name__ == "__main__":
    # important! we use this to mimic start a module as a script
    fire.Fire({"start": start})