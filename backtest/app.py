import asyncio
import logging
from html.entities import name2codepoint

import cfg4py
import omicron
from sanic import Sanic

from backtest.broker import Broker
from backtest.config import get_config_dir

app = Sanic("backtest")
logger = logging.getLogger(__name__)


class BackTest:
    async def init(self, *args):
        logger.info("init backtest server: %s", self.__class__.__name__)
        await omicron.init()


def start(port: int = 7080):
    cfg = cfg4py.init(get_config_dir())

    name = cfg.account.name
    cash = cfg.account.cash
    commission = cfg.account.commission

    broker = Broker(name, cash, commission)
    app.ctx.broker = broker

    bt = BackTest()

    app.register_listener(bt.init, "before_server_start")
    app.run(host="0.0.0.0", port=port, register_sys_signals=True)
    logger.info("backtest server stopped")
