import unittest
from turtle import pos

from tests import get, post, start_backtest_server


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
                "order_time": "2022-03-01 10:04:00",
                "request_id": "123456789",
            },
        )

        self.assertEqual(response["status"], 0)

        data = response["data"]
        self.assertEqual(data["security"], "002537.XSHE")
        self.assertAlmostEqual(data["price"], 9.420000076293945, 2)
        self.assertEqual(data["shares"], 500)

    async def test_position(self):
        response = await get("position")
        self.assertListEqual([], response["data"])

        await post(
            "buy",
            {
                "security": "002537.XSHE",
                "price": 10,
                "volume": 500,
                "timeout": 0.5,
                "order_time": "2022-03-01 10:04:00",
                "request_id": "123456789",
            },
        )

        response = await get("position")
        position = response["data"][0]
        self.assertEqual(position["security"], "002537.XSHE")
        self.assertAlmostEqual(position["shares"], 500)
        self.assertAlmostEqual(position["price"], 9.42, 2)

        response = await get("position", {"date": "2022-03-07"})
        position = response["data"][0]

        self.assertAlmostEqual(position["security"], "002537.XSHE")
        self.assertAlmostEqual(position["shares"], 500)
        self.assertAlmostEqual(position["price"], 9.42, 2)

    async def test_balance(self):
        balance = (await get("balance"))["data"]
        self.assertEqual(balance["cash"], 1_000_000)
        self.assertEqual(balance["total"], 1_000_000)
        self.assertAlmostEqual(balance["pnl"], 0, 2)

        await post(
            "buy",
            {
                "security": "002537.XSHE",
                "price": 10,
                "volume": 500,
                "timeout": 0.5,
                "order_time": "2022-03-01 10:04:00",
                "request_id": "123456789",
            },
        )

        balance = (await get("balance"))["data"]
        self.assertAlmostEqual(balance["cash"], 995289.53, 2)
        self.assertAlmostEqual(balance["market_value"], 4750.0, 2)
        self.assertAlmostEqual(balance["total"], 1000039.5289618492, 2)
        self.assertAlmostEqual(balance["pnl"], 39.53, 2)
        self.assertAlmostEqual(balance["ppnl"], 39.53 / 1_000_000, 2)
