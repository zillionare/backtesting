import datetime
import unittest
import uuid
from unittest import mock

import arrow
import cfg4py

from backtest.app import application_exit, application_init
from backtest.common.helper import jsonify
from tests import (
    assert_deep_almost_equal,
    data_populate,
    delete,
    get,
    init_interface_test,
    post,
)

app = init_interface_test()


class InterfacesTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        name = "test"
        capital = 1_000_000
        commission = 1e-4
        self.token = uuid.uuid4().hex

        cfg = cfg4py.get_instance()
        self.admin_token = cfg.auth.admin

        await data_populate()

        await delete("accounts", self.admin_token)

        response = await post(
            "accounts",
            self.admin_token,
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

    async def test_accounts(self):
        """create, list and delete accounts"""

        await post(
            "accounts",
            self.admin_token,
            data={
                "name": "test2",
                "capital": 1_000_000,
                "commission": 1e-4,
                "token": uuid.uuid4().hex,
            },
        )

        response = await get("accounts", self.admin_token)
        self.assertEqual(2, len(response["data"]))
        self.assertEqual(response["status"], 0)
        self.assertEqual(response["data"][0]["account_name"], "test")

        res = await delete("accounts", self.admin_token, params={"name": "test2"})
        self.assertEqual(res["data"], 1)

    async def test_status(self):
        response = await get("status", self.token)
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

    async def test_market_buy(self):
        response = await post(
            "market_buy",
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

    async def test_market_sell(self):
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
            "market_sell",
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

        response = await get("positions", self.token, date="2022-03-07")
        position = response["data"][0]

        self.assertAlmostEqual(position["security"], "002537.XSHE")
        self.assertAlmostEqual(position["shares"], 500)
        self.assertAlmostEqual(position["price"], 9.42, 2)

    @mock.patch("arrow.now", return_value=arrow.get("2022-03-14 15:00:00"))
    async def test_balance(self, mock_now):
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
        self.assertAlmostEqual(balance["available"], 995289.5289618492, 2)
        self.assertAlmostEqual(balance["market_value"], 4750.0, 2)
        self.assertAlmostEqual(balance["total"], 1000039.528, 2)
        self.assertAlmostEqual(balance["pnl"], 39.5289, 2)
        self.assertAlmostEqual(balance["ppnl"], 39.5289 / 1_000_000, 2)

        info = (await get("info", self.token))["data"]
        self.assertEqual(info["start"], "2022-03-01")
        self.assertAlmostEqual(info["assets"], 1000069.528, 2)
        self.assertAlmostEqual(info["earnings"], 69.52896, 2)

        available_money = (await get("available_money", self.token))["data"]
        self.assertAlmostEqual(995289.5289, available_money, 2)

        available_shares = (await get("available_shares", self.token))["data"]
        self.assertEqual(available_shares["002537.XSHE"], 0)

        metrics = (await get("metrics", self.token))["data"]
        self.assertEqual(metrics["total_tx"], 0)

        returns = (await get("returns", self.token))["data"]
        print(returns)

    @mock.patch("arrow.now", return_value=arrow.get("2022-03-14 15:00:00"))
    async def test_metrics(self, mocked_now):
        hljh = "002537.XSHE"

        for price, volume, tm in [
            (9.13, 500, "2022-03-01 09:31:00"),
            (10.03, 500, "2022-03-02 09:31:00"),
            (11.05, 500, "2022-03-03 09:31:00"),
            (10.47, 500, "2022-03-04 09:31:00"),
            (9.41, 500, "2022-03-07 09:31:00"),
            (9.57, 500, "2022-03-08 09:31:00"),
            (9.08, 500, "2022-03-09 09:31:00"),
            (9.1, 500, "2022-03-10 09:31:00"),
            (9.65, 500, "2022-03-11 09:31:00"),
            (9.65, 500, "2022-03-14 09:31:00"),
        ]:
            await post(
                "buy",
                self.token,
                {
                    "security": hljh,
                    "price": price,
                    "volume": volume,
                    "timeout": 0.5,
                    "order_time": tm,
                    "request_id": uuid.uuid4().hex,
                },
            )

        await post(
            "sell",
            self.token,
            {
                "security": hljh,
                "price": 9.1,
                "volume": 5000,
                "order_time": "2022-03-14 15:00:00",
                "request_id": uuid.uuid4().hex,
            },
        )

        actual = (await get("metrics", self.token, baseline=hljh))["data"]
        exp = {
            "start": datetime.date(2022, 3, 1),
            "end": datetime.date(2022, 3, 14),
            "window": 10,
            "total_tx": 9,
            "total_profit": -779.1568067073822,
            "total_profit_rate": -0.0007791568067073822,
            "win_rate": 0.4444444444444444,
            "mean_return": -6.952228828114507e-05,
            "sharpe": -1.7461,
            "sortino": -2.51418,
            "calmar": -3.9873290,
            "max_drawdown": -0.004438,
            "annual_return": -0.017698244,
            "volatility": 0.02721410,
            "baseline": {
                "code": hljh,
                "win_rate": 0.5555555555555556,
                "sharpe": 0.6190437353475076,
                "max_drawdown": -0.17059373779725692,
                "sortino": 1.0015572769806516,
                "annual_return": 0.19278435493450163,
                "total_profit_rate": 0.006315946578979492,
                "volatility": 1.1038380776228978,
            },
        }

        assert_deep_almost_equal(self, jsonify(exp), actual, places=2)

    async def test_start_backtest(self):
        "test start_backtest"
        await post(
            "start_backtest",
            "",
            {
                "name": "test_start_backtest",
                "version": 0.1,
                "start": "2022-03-01",
                "end": "2022-03-14",
                "capital": 1_000_000,
                "commission": 1e-4,
            },
        )

        accounts = await get("accounts", self.admin_token)
        print(accounts)

    async def test_protect_admin(self):
        """valid admin token is tested through other tests"""
        response = await post(
            "accounts", "invalid_token", {"username": "test", "password": "test"}
        )
        self.assertEqual(response["msg"], "token is invalid")

    async def test_bills(self):
        await delete("accounts", self.admin_token)

        _ = await post(
            "start_backtest",
            self.admin_token,
            data={
                "name": "test_bill",
                "capital": 1_000_000,
                "commission": 1e-4,
                "token": self.token,
                "start": "2022-03-01",
                "end": "2022-03-14",
            },
        )

        r = ((await get("bills", self.token)) or {}).get("data")
        self.assertIn("tx", r)
        self.assertIn("trades", r)
        self.assertIn("positions", r)
        self.assertIn("assets", r)

    async def test_sell_percent(self):
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
            "sell_percent",
            self.token,
            {
                "security": "002537.XSHE",
                "price": None,
                "percent": 0.5,
                "order_time": "2022-03-03 10:04:00",
            },
        )

        result = response["data"]

        # should be 250, rounded to 300
        self.assertEqual(300, result[0]["volume"])

    async def test_metrics_2(self):
        code = "600919.XSHG"
        price = 7.02
        volume = 300

        _ = await post(
            "buy",
            self.token,
            {
                "security": code,
                "price": price,
                "volume": volume,
                "order_time": "2022-3-1 10:35:00",
            },
        )

        await post(
            "sell",
            self.token,
            {
                "security": code,
                "price": 6.83,
                "volume": volume,
                "order_time": "2022-3-3 09:31:00",
            },
        )

        metrics = await get("metrics", self.token, start="2022-3-1", end="2022-3-3")
        print(metrics)
