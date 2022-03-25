import os
import unittest

import cfg4py

from backtest.app import application_init
from backtest.config import get_config_dir
from backtest.feed.filefeed import FileFeed
from backtest.feed.zillionarefeed import ZillionareFeed


class AppTest(unittest.IsolatedAsyncioTestCase):
    async def test_application_init(self):
        os.environ[cfg4py.envar] = "DEV"
        cfg = cfg4py.init(get_config_dir())

        cfg.feed.type = "zillionare"

        from backtest.app import application

        await application_init(application)

        self.assertTrue(isinstance(application.ctx.feed, ZillionareFeed))

        cfg.feed.type = "file"
        await application_init(application)
        self.assertTrue(isinstance(application.ctx.feed, FileFeed))
