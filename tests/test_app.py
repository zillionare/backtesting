import os
import unittest

import cfg4py
import pkg_resources

from backtest.app import application_init
from backtest.feed.zillionarefeed import ZillionareFeed


class AppTest(unittest.IsolatedAsyncioTestCase):
    async def test_application_init(self):
        os.environ[cfg4py.envar] = "DEV"

        from backtest.app import application

        await application_init(application)

        self.assertTrue(isinstance(application.ctx.feed, ZillionareFeed))

    async def test_root(self):
        os.environ[cfg4py.envar] = "DEV"
        from tests import init_interface_test

        ver = pkg_resources.get_distribution("zillionare-backtest").parsed_version

        app = init_interface_test()
        cfg = cfg4py.get_instance()

        _, response = await app.asgi_client.get("/")
        endpoint = cfg.server.prefix.rstrip("/")
        endpoint = f"{endpoint}/v{ver.major}.{ver.minor}"
        self.assertEqual(response.status, 200)
        self.assertEqual(
            response.text,
            f"Welcome to zillionare bactest server. The endpoints is {endpoint}",
        )
