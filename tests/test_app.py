import os
import unittest

import cfg4py

import backtest
from backtest.app import application_exit, application_init
from backtest.feed.zillionarefeed import ZillionareFeed


class AppTest(unittest.IsolatedAsyncioTestCase):
    async def test_application_init(self):
        os.environ[cfg4py.envar] = "DEV"

        from backtest.app import application

        await application_init(application)

        self.assertTrue(isinstance(application.ctx.feed, ZillionareFeed))
        await application_exit(application)

    async def test_root_path(self):
        os.environ[cfg4py.envar] = "DEV"

        _, response = await backtest.app.application.asgi_client.get("/")

        self.assertEqual(response.status, 200)
        self.assertSetEqual(
            set(response.json.keys()), set(["greetings", "version", "endpoint"])
        )
