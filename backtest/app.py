"""Main module."""
import logging

import cfg4py
import fire
import omicron
from omicron.models.timeframe import TimeFrame
from pyemit import emit
from sanic import Sanic

from backtest.config import get_config_dir
from backtest.feed.basefeed import BaseFeed
from backtest.web.accounts import Accounts
from backtest.web.interfaces import bp

application = Sanic("backtest")
logger = logging.getLogger(__name__)


@application.listener("before_server_start")
async def application_init(app, *args):
    try:
        await omicron.init()
    except Exception:
        logger.warning(
            "omicron running in degrade mode, this may cause inaccurate results due to calendar issues"
        )
        TimeFrame.service_degrade()

    cfg = cfg4py.get_instance()
    await emit.start(emit.Engine.REDIS, start_server=True, dsn=cfg.redis.dsn)
    feed = await BaseFeed.create_instance(interface="zillionare")
    await feed.init()

    app.ctx.feed = feed
    app.ctx.accounts = Accounts()
    app.ctx.accounts.on_startup()


@application.listener("after_server_stop")
async def application_exit(app, *args):
    await omicron.close()
    await emit.stop()
    accounts = app.ctx.accounts
    accounts.on_exit()


def start(port: int):
    cfg = cfg4py.init(get_config_dir())

    path = cfg.server.path.rstrip("/")

    logger.info("start backtest server at http://host:%s/%s", port, path)
    bp.url_prefix = path

    application.blueprint(bp)

    application.run(host="0.0.0.0", port=port, register_sys_signals=True)
    logger.info("backtest server stopped")


if __name__ == "__main__":
    # important! we use this to mimic start a module as a script
    fire.Fire({"start": start})
