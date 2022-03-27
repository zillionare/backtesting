import unittest
import uuid

from backtest.app import application_init
from tests import get, init_interface_test, post

app = init_interface_test()


class InterfacesTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        name = "test"
        capital = 1_000_000
        commission = 0.001
        self.token = uuid.uuid4().hex

        await application_init(app)

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
        self.assertEqual(data["volume"], 500)

    async def test_sell(self):
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

        response = await post(
            "sell",
            self.token,
            {
                "security": "002537.XSHE",
                "price": 10,
                "volume": 500,
                "order_time": "2022-03-02 10:04:00",
            },
        )

        tx = response["data"][0]
        self.assertEqual(tx["security"], "002537.XSHE")
        self.assertEqual(tx["volume"], 500)
        self.assertAlmostEqual(tx["price"], 10.45, 2)

    async def test_position(self):
        response = await get("positions", self.token)
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

        response = await get("positions", self.token)
        position = response["data"][0]
        self.assertEqual(position["security"], "002537.XSHE")
        self.assertAlmostEqual(position["shares"], 500)
        self.assertAlmostEqual(position["price"], 9.42, 2)

        response = await get("positions", self.token, {"date": "2022-03-07"})
        position = response["data"][0]

        self.assertAlmostEqual(position["security"], "002537.XSHE")
        self.assertAlmostEqual(position["shares"], 500)
        self.assertAlmostEqual(position["price"], 9.42, 2)

    async def test_balance(self):
        "this also test info, available_money, available_shares, metrics, get_returns"
        balance = (await get("balance", self.token))["data"]
        self.assertEqual(balance["available"], 1_000_000)
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
        self.assertAlmostEqual(balance["available"], 995285.289, 2)
        self.assertAlmostEqual(balance["market_value"], 4750.0, 2)
        self.assertAlmostEqual(balance["total"], 1000035.289, 2)
        self.assertAlmostEqual(balance["pnl"], 35.289, 2)
        self.assertAlmostEqual(balance["ppnl"], 35.289 / 1_000_000, 2)

        info = (await get("info", self.token))["data"]
        self.assertEqual(info["start"], "2022-03-01")
        self.assertAlmostEqual(info["assets"], 1000035.289, 2)
        self.assertAlmostEqual(info["earnings"], 35.289, 2)

        available_money = (await get("available_money", self.token))["data"]
        self.assertAlmostEqual(995285.289, available_money, 2)

        available_shares = (await get("available_shares", self.token))["data"]
        self.assertEqual(available_shares["002537.XSHE"], 0)

        metrics = (await get("metrics", self.token))["data"]
        self.assertEqual(metrics["total_tx"], 0)

        returns = (await get("returns", self.token))["data"]
        print(returns)
