import os
import unittest

import cfg4py

from backtest.app import application_init
from backtest.config import get_config_dir
from backtest.feed.filefeed import FileFeed
from backtest.feed.zillionarefeed import ZillionareFeed
from tests import data_dir


class AppTest(unittest.IsolatedAsyncioTestCase):
    async def test_application_init(self):
        os.environ[cfg4py.envar] = "DEV"
        cfg = cfg4py.init(get_config_dir())

        cfg.feed.type = "zillionare"

        from backtest.app import application

        await application_init(application)

        self.assertTrue(isinstance(application.ctx.feed, ZillionareFeed))

        cfg.feed.type = "file"
        cfg.feed.filefeed.bars_path = os.path.join(data_dir(), "bars_1m.pkl")
        cfg.feed.filefeed.limits_path = os.path.join(data_dir(), "limits.pkl")
        await application_init(application)
        self.assertTrue(isinstance(application.ctx.feed, FileFeed))
