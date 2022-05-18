import os
import unittest

import cfg4py

from backtest.app import application_init
from backtest.config import get_config_dir
from backtest.feed.zillionarefeed import ZillionareFeed
from tests import data_dir


class AppTest(unittest.IsolatedAsyncioTestCase):
    async def test_application_init(self):
        os.environ[cfg4py.envar] = "DEV"

        from backtest.app import application

        await application_init(application)

        self.assertTrue(isinstance(application.ctx.feed, ZillionareFeed))
