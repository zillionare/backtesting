import datetime
import os
import unittest
import uuid
from unittest import mock

import arrow
import cfg4py

from backtest.common.errors import Error
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
        self.name = "test"
        principal = 1_000_000
        commission = 1e-4
        self.token = uuid.uuid4().hex

        cfg = cfg4py.get_instance()
        self.admin_token = cfg.auth.admin

        try:
            os.remove("/var/log/backtest/entrust.log")
            os.remove("/var/log/backtest/trade.log")
        except FileNotFoundError:
            pass

        await data_populate()

        await delete("accounts", self.admin_token)

        response = await post(
            "start_backtest",
            self.token,
            data={
                "name": self.name,
                "principal": principal,
                "commission": commission,
                "token": self.token,
                "start": "2022-03-01",
                "end": "2022-03-14",
            },
        )

        self.assertEqual(response["account_name"], "test")
        self.assertEqual(response["principal"], principal)
        self.assertEqual(response["token"], self.token)

        return await super().asyncSetUp()

    async def test_accounts(self):
        """create, list and delete accounts"""

        await post(
            "start_backtest",
            self.token,
            data={
                "name": "test2",
                "principal": 1_000_000,
                "commission": 1e-4,
                "token": uuid.uuid4().hex,
                "start": "2022-02-28",
                "end": "2022-03-14",
            },
        )

        response = await get("accounts", self.admin_token)
        self.assertEqual(2, len(response))
        self.assertEqual(response[0]["account_name"], "test")

        # should raise no Error
        await delete("accounts", self.admin_token, params={"name": "test2"})

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

        self.assertEqual(response["security"], "002537.XSHE")
        self.assertAlmostEqual(response["price"], 9.420000076293945, 2)
        self.assertEqual(response["filled"], 500)

    async def test_market_buy(self):
        response = await post(
            "market_buy",
            self.token,
            {
                "security": "002537.XSHE",
                "volume": 500,
                "timeout": 0.5,
                "order_time": "2022-03-01 10:04:00",
                "request_id": "123456789",
            },
        )

        self.assertEqual(response["security"], "002537.XSHE")
        self.assertAlmostEqual(response["price"], 9.420000076293945, 2)
        self.assertEqual(response["filled"], 500)

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

        tx = response[0]
        self.assertEqual(tx["security"], "002537.XSHE")
        self.assertEqual(tx["filled"], 500)
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

        tx = response[0]
        self.assertEqual(tx["security"], "002537.XSHE")
        self.assertEqual(tx["filled"], 500)
        self.assertAlmostEqual(tx["price"], 10.45, 2)

    async def test_position(self):
        response = await get("positions", self.token)
        self.assertEqual(0, len(response))

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
        position = response[0]
        self.assertEqual(position["security"], "002537.XSHE")
        self.assertAlmostEqual(position["shares"], 500)
        self.assertAlmostEqual(position["price"], 9.42, 2)

        response = await get("positions", self.token, date="2022-03-07")
        position = response[0]

        self.assertAlmostEqual(position["security"], "002537.XSHE")
        self.assertAlmostEqual(position["shares"], 500)
        self.assertAlmostEqual(position["price"], 9.42, 2)

    @mock.patch("arrow.now", return_value=arrow.get("2022-03-14 15:00:00"))
    async def test_info(self, mock_now):
        balance = await get("info", self.token)
        self.assertEqual(balance["available"], 1_000_000)
        self.assertEqual(balance["assets"], 1_000_000)
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

        info = await get("info", self.token)
        self.assertAlmostEqual(info["available"], 995289.5289618492, 2)
        self.assertAlmostEqual(info["market_value"], 4780.0, 2)
        self.assertAlmostEqual(info["assets"], 1000069.528, 2)
        self.assertAlmostEqual(info["pnl"], 69.5289, 2)
        self.assertAlmostEqual(info["ppnl"], 69.5289 / 1_000_000, 2)

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
            (9.65, 500, "2022-03-11 09:31:00"),  # 这一笔不会成交
            (9.65, 500, "2022-03-14 09:31:00"),
        ]:
            try:
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
            except Error:
                pass

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

        actual = await get("metrics", self.token, baseline=hljh)
        exp = {
            "start": datetime.date(2022, 3, 1),
            "end": datetime.date(2022, 3, 14),
            "window": 10,
            "total_tx": 8,
            "total_profit": -343.13999999978114,
            "total_profit_rate": -0.0003431399999997811,
            "win_rate": 0.625,
            "mean_return": -2.9972019661695835e-05,
            "sharpe": -1.439194474227825,
            "sortino": -2.1212244527176227,
            "calmar": -1.8723514035186801,
            "max_drawdown": -0.0041827334569883405,
            "annual_return": -0.00783154685873666,
            "volatility": 0.026093033031478072,
            "baseline": {
                "code": "002537.XSHE",
                "win_rate": 0.5555555555555556,
                "sharpe": 0.6190437353475076,
                "max_drawdown": -0.17059373779725692,
                "sortino": 1.0015572769806516,
                "annual_return": 0.19278435493450163,
                "total_profit_rate": 0.006315946578979492,
                "volatility": 1.1038380776228978,
            },
        }
        assert_deep_almost_equal(self, exp, actual, places=2)

    async def test_protect_admin(self):
        """valid admin token is tested through other tests"""
        response = await get("accounts", "invalid_token")
        self.assertIsNone(response)

    async def test_bills(self):
        await delete("accounts", self.admin_token)

        _ = await post(
            "start_backtest",
            self.admin_token,
            data={
                "name": "test_bill",
                "principal": 1_000_000,
                "commission": 1e-4,
                "token": self.token,
                "start": "2022-03-01",
                "end": "2022-03-14",
            },
        )

        r = (await get("bills", self.token)) or {}
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

        # should be 250, rounded to 300
        self.assertEqual(300, response[0]["filled"])

    async def test_delete_accounts(self):
        # the account is created by asyncSetup
        await delete("accounts", self.token, params={"name": self.name})

    async def test_frozen_accounts(self):
        with self.assertRaises(Error) as cm:
            await post(
                "buy",
                self.token,
                {
                    "security": "002537.XSHE",
                    "price": 10,
                    "volume": 500,
                    "timeout": 0.5,
                    "order_time": "2022-05-01 10:04:00",
                    "request_id": "123456789",
                },
            )
        self.assertTrue(str(cm.exception).find("冻结") > 0)
