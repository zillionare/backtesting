"""Main module."""
import logging

import cfg4py
import fire
import omicron
from omicron.models.timeframe import TimeFrame
from sanic import Sanic

from backtest.config import get_config_dir
from backtest.feed.basefeed import BaseFeed
from backtest.web.accounts import Accounts
from backtest.web.interfaces import bp

application = Sanic("backtest")
logger = logging.getLogger(__name__)


async def application_init(app, *args):
    logger.info("init backtest server")
    try:
        await omicron.init()
    except Exception:
        TimeFrame.service_degrade()

    feed = await BaseFeed.create_instance(interface="zillionare")
    app.ctx.feed = feed
    app.ctx.accounts = Accounts()


def start(port: int):
    cfg = cfg4py.init(get_config_dir())

    path = cfg.server.path.rstrip("/")

    logger.info("start backtest server at http://host:%s/%s", port, path)
    bp.url_prefix = path

    application.blueprint(bp)

    application.register_listener(application_init, "before_server_start")
    application.run(host="0.0.0.0", port=port, register_sys_signals=True)
    logger.info("backtest server stopped")


if __name__ == "__main__":
    # important! we use this to mimic start a module as a script
    fire.Fire({"start": start})
