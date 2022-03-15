import unittest

from tests import post, start_backtest_server


class ApiTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = await start_backtest_server()
        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        if self.server:
            self.server.kill()

    async def test_status(self):
        response = await post("status", {})
        self.assertEqual("ok", response["status"])
