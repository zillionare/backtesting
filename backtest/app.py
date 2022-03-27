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


@application.listener("before_server_start")
async def application_init(app, *args):
    cfg = cfg4py.get_instance()

    logger.info("init backtest server with feed: %s", cfg.feed.type)
    if cfg.feed.type == "zillionare":
        try:
            await omicron.init()
        except Exception:
            TimeFrame.service_degrade()

        feed = await BaseFeed.create_instance(interface="zillionare")
        await feed.init()
    elif cfg.feed.type == "file":
        feed = await BaseFeed.create_instance(
            interface="file",
            bars_for_match_path=cfg.feed.filefeed.bars_path,
            price_limits_path=cfg.feed.filefeed.limits_path,
        )
        await feed.init()
    else:
        raise Exception("datasource type not supported")

    app.ctx.feed = feed
    app.ctx.accounts = Accounts()
    app.ctx.accounts.on_startup()


@application.listener("after_server_stop")
async def application_exit(app, *args):
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
