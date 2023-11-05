"""Main module."""
import os
import sys

import cfg4py
import fire
import omicron
from omicron.core.backtestlog import BacktestLogger
from omicron.models.timeframe import TimeFrame
from pyemit import emit
from sanic import Sanic, response

from backtest.config import endpoint, get_config_dir
from backtest.feed.basefeed import BaseFeed
from backtest.web.accounts import Accounts
from backtest.web.interfaces import bp, ver

application = Sanic("backtest")
logger = BacktestLogger.getLogger(__name__)


@application.route("/")
async def root(request):
    return response.json(
        {
            "greetings": "欢迎使用大富翁回测系统！",
            "version": ver.base_version,
            "endpoint": bp.url_prefix,
        }
    )


@application.listener("before_server_start")
async def application_init(app, *args):
    try:
        await omicron.init()
    except Exception:
        logger.warning(
            "omicron running in degrade mode, this may cause inaccurate results due to calendar issues"
        )
        if os.environ.get(cfg4py.envar) in ("DEV", "TEST"):
            TimeFrame.service_degrade()
        else:
            sys.exit(-1)

    cfg = cfg4py.get_instance()
    await emit.start(emit.Engine.REDIS, start_server=True, dsn=cfg.redis.dsn)
    feed = await BaseFeed.create_instance(interface="zillionare")
    await feed.init()

    app.ctx.feed = feed
    app.ctx.accounts = Accounts()
    app.ctx.accounts.on_startup()


@application.listener("after_server_stop")
async def application_exit(app, *args):
    accounts = app.ctx.accounts
    accounts.on_exit()
    await omicron.close()
    await emit.stop()


def start(port: int):
    cfg4py.init(get_config_dir())
    ep = endpoint()
    logger.info("start backtest server at http://host:%s/%s", port, ep)
    bp.url_prefix = ep

    # added for gh://zillionare/backtesting/issues/6
    application.config.RESPONSE_TIMEOUT = 60 * 10
    application.blueprint(bp)

    application.run(host="0.0.0.0", port=port, register_sys_signals=True)
    logger.info("backtest server stopped")


if __name__ == "__main__":
    # important! we use this to mimic start a module as a script
    fire.Fire({"start": start})
