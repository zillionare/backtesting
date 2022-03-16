import unittest

from tests import post, start_backtest_server


class InterfacesTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = await start_backtest_server()
        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        if self.server:
            self.server.kill()

    async def test_status(self):
        response = await post("status", {})
        self.assertEqual("ok", response["status"])

    async def test_buy(self):
        response = await post(
            "buy",
            {
                "security": "002537.XSHE",
                "price": 10,
                "volume": 500,
                "timeout": 0.5,
                "order_time": "2022-03-01 09:41:00",
                "request_id": "123456789",
            },
        )

        print(response)
