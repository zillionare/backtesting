import unittest
import uuid

from tests import get, post, start_backtest_server


class InterfacesTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = await start_backtest_server()

        name = "test"
        capital = 1_000_000
        commission = 0.001
        self.token = uuid.uuid4().hex

        response = await post(
            "accounts",
            self.token,
            data={
                "name": name,
                "capital": capital,
                "commission": commission,
                "token": self.token,
            },
        )

        self.assertEqual(response["status"], 0)
        self.assertEqual(response["data"]["account_name"], name)

        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        if self.server:
            self.server.kill()

    async def test_list_accounts(self):
        response = await get("accounts", self.token)
        self.assertEqual(response["status"], 0)
        self.assertEqual(response["data"][0]["account_name"], "test")

    async def test_status(self):
        response = await post("status", self.token, {})
        self.assertEqual("ok", response["status"])

    async def test_buy(self):
        response = await post(
            "buy",
            self.token,
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
        response = await get("position", self.token)
        self.assertListEqual([], response["data"])

        await post(
            "buy",
            self.token,
            {
                "security": "002537.XSHE",
                "price": 10,
                "volume": 500,
                "timeout": 0.5,
                "order_time": "2022-03-01 10:04:00",
                "request_id": "123456789",
            },
        )

        response = await get("position", self.token)
        position = response["data"][0]
        self.assertEqual(position["security"], "002537.XSHE")
        self.assertAlmostEqual(position["shares"], 500)
        self.assertAlmostEqual(position["price"], 9.42, 2)

        response = await get("position", self.token, {"date": "2022-03-07"})
        position = response["data"][0]

        self.assertAlmostEqual(position["security"], "002537.XSHE")
        self.assertAlmostEqual(position["shares"], 500)
        self.assertAlmostEqual(position["price"], 9.42, 2)

    async def test_balance(self):
        "this also test info, available_money, available_shares, metrics"
        balance = (await get("balance", self.token))["data"]
        self.assertEqual(balance["cash"], 1_000_000)
        self.assertEqual(balance["total"], 1_000_000)
        self.assertAlmostEqual(balance["pnl"], 0, 2)

        await post(
            "buy",
            self.token,
            {
                "security": "002537.XSHE",
                "price": 10,
                "volume": 500,
                "timeout": 0.5,
                "order_time": "2022-03-01 10:04:00",
                "request_id": "123456789",
            },
        )

        balance = (await get("balance", self.token))["data"]
        self.assertAlmostEqual(balance["cash"], 995285.289, 2)
        self.assertAlmostEqual(balance["market_value"], 4750.0, 2)
        self.assertAlmostEqual(balance["total"], 1000035.289, 2)
        self.assertAlmostEqual(balance["pnl"], 35.289, 2)
        self.assertAlmostEqual(balance["ppnl"], 35.289 / 1_000_000, 2)

        info = (await get("info", self.token))["data"]
        self.assertEqual(info["start"], "2022-03-01T10:04:00")
        self.assertAlmostEqual(info["assets"], 1000035.289, 2)
        self.assertAlmostEqual(info["earnings"], 35.289, 2)

        available_money = (await get("available_money", self.token))["data"]
        self.assertAlmostEqual(995285.289, available_money, 2)

        available_shares = (await get("available_shares", self.token))["data"]
        self.assertEqual(available_shares["002537.XSHE"], 0)

        metrics = (await get("metrics", self.token))["data"]
        self.assertEqual(metrics["total_tx"], 0)
