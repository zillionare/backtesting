"""Main module."""
import asyncio
import logging
from html.entities import name2codepoint

import cfg4py
import omicron
from sanic import Sanic

from backtest.broker import Broker
from backtest.config import get_config_dir
from backtest.feed.basefeed import BaseFeed

app = Sanic("backtest")
logger = logging.getLogger(__name__)


async def application_init(self, app, *args):
    logger.info("init backtest server: %s", self.__class__.__name__)
    await omicron.init()
    feed = await BaseFeed.create_instance(interface="zillionare")
    app.ctx.feed = feed


def start(port: int = 7080):
    cfg = cfg4py.init(get_config_dir())

    name = cfg.account.name
    cash = cfg.account.cash
    commission = cfg.account.commission

    broker = Broker(name, cash, commission)
    app.ctx.broker = broker
    app.ctx.cfg = cfg

    app.register_listener(application_init, "before_server_start")
    app.run(host="0.0.0.0", port=port, register_sys_signals=True)
    logger.info("backtest server stopped")
