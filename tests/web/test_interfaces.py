import datetime
import os
import unittest
import uuid
from unittest import mock

import arrow
import cfg4py
import numpy as np
import omicron
import pandas as pd
from coretypes.errors.trade import BadParamsError, PriceNotMeet
from coretypes.errors.trade.base import TradeError
from omicron import tf
from pyemit import emit

from tests import (
    assert_deep_almost_equal,
    data_populate,
    delete,
    get,
    init_interface_test,
    post,
)

app = init_interface_test()

hljh = "002537.XSHE"


class InterfacesTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        principal = 1_000_000
        commission = 1e-4
        self.token = uuid.uuid4().hex
        self.name = "test" + self.token[:4]

        cfg = cfg4py.get_instance()
        self.admin_token = cfg.auth.admin

        try:
            os.remove("/var/log/backtest/entrust.log")
            os.remove("/var/log/backtest/trade.log")
            os.remove("/tmp/backtest/backtest.index.json")
        except FileNotFoundError:
            pass

        try:
            await omicron.init()
        except Exception:
            tf.service_degrade()

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

        self.assertEqual(response["account_name"], self.name)
        self.assertEqual(response["principal"], principal)
        self.assertEqual(response["token"], self.token)

        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        await omicron.close()
        await emit.stop()

        return await super().asyncTearDown()

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
        self.assertEqual(response[1]["account_name"], "test2")

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
                "volume": 250,
                "order_time": "2022-03-02 10:04:00",
            },
        )

        tx = response[0]
        self.assertEqual(tx["security"], "002537.XSHE")
        self.assertEqual(tx["filled"], 200)
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
        self.assertEqual(position["sellable"], 0)

        response = await get("positions", self.token, date="2022-03-07")
        position = response[0]

        self.assertAlmostEqual(position["security"], "002537.XSHE")
        self.assertAlmostEqual(position["shares"], 500)
        self.assertAlmostEqual(position["price"], 9.42, 2)

        # issue 8, sell them all and check positions
        await post(
            "sell",
            self.token,
            {
                "security": "002537.XSHE",
                "price": 9.85,
                "volume": 500,
                "order_time": "2022-03-02 10:04:00",
            },
        )
        response = await get("positions", self.token, date="2022-03-07")
        self.assertEqual(0, response.size)

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
        self.assertAlmostEqual(info["market_value"], 4750.0, 2)
        self.assertAlmostEqual(info["assets"], 1000039.528, 2)
        self.assertAlmostEqual(info["pnl"], 39.5289, 2)
        self.assertAlmostEqual(info["ppnl"], 39.5289 / 1_000_000, 2)

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
            (9.68, 500, "2022-03-11 09:31:00"),
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
            except TradeError:
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

        await post("stop_backtest", self.token, data={})
        actual = await get("metrics", self.token, baseline=hljh)
        exp = {
            "start": datetime.date(2022, 3, 1),
            "end": datetime.date(2022, 3, 14),
            "window": 10,
            "total_tx": 9,
            "total_profit": -404.0999999998603,
            "total_profit_rate": -0.0004040999999998603,
            "win_rate": 0.5555555555555556,
            "mean_return": -3.896698729980441e-05,
            "sharpe": -1.396890251070207,
            "sortino": -2.0486727998320817,
            "calmar": -2.422741706100782,
            "max_drawdown": -0.0041827334569883405,
            "annual_return": -0.010133682791748755,
            "volatility": 0.02850594795764624,
            "baseline": {
                "total_profit_rate": 0.006315946578979492,
                "win_rate": 0.5555555555555556,
                "mean_return": 0.0028306511230766773,
                "sharpe": 0.6190437353475076,
                "sortino": 1.0015572769806516,
                "calmar": 1.1300787322194514,
                "max_drawdown": -0.17059373779725692,
                "annual_return": 0.19278435493450163,
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
            "buy",
            self.token,
            {
                "security": "002537.XSHE",
                "price": 9.13,
                "volume": 500,
                "timeout": 0.5,
                "order_time": "2022-03-01 09:31:00",
                "request_id": "123456789",
            },
        )

        _ = await post(
            "sell",
            self.token,
            {
                "security": "002537.XSHE",
                "price": 9.1,
                "volume": 100,
                "timeout": 0.5,
                "order_time": "2022-03-14 15:00:00",
            },
        )

        _ = await post("stop_backtest", self.token, data={})
        bills = (await get("bills", self.token)) or {}
        tx = bills["tx"][0]
        self.assertEqual(tx["shares"], 100)
        self.assertAlmostEqual(tx["fee"], 0.19, 2)
        self.assertEqual(tx["sec"], hljh)
        self.assertEqual(tx["window"], 10)
        self.assertAlmostEqual(tx["entry_price"], 9.09, 2)
        self.assertAlmostEqual(tx["exit_price"], 9.56, 2)
        self.assertEqual(tx["exit_time"], "2022-03-14T15:00:00")

        trades = bills["trades"]
        for _, v in trades.items():
            if v["order_side"] == "买入":
                self.assertAlmostEqual(v["trade_fees"], 0.45, 2)
                self.assertAlmostEqual(v["price"], 9.09, 2)
                self.assertEqual(v["filled"], 500)
            if v["order_side"] == "卖出":
                self.assertAlmostEqual(v["trade_fees"], 0.1, 2)
                self.assertAlmostEqual(v["price"], 9.56, 2)
                self.assertAlmostEqual(v["filled"], 100)

        positions = bills["positions"]
        df = pd.DataFrame(
            positions, columns=["frame", "security", "shares", "sellable", "price"]
        )
        np.testing.assert_array_equal(df["shares"], [0, *([500] * 9), 400])

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
        self.assertEqual(200, response[0]["filled"])

    async def test_delete_accounts(self):
        # the account is created by asyncSetup
        await delete("accounts", self.token, params={"name": self.name})

    async def test_frozen_accounts(self):
        with self.assertRaises(BadParamsError) as cm:
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

    async def test_stop_backtest(self):
        info = await get("info", self.token)
        self.assertEqual(info["bt_stopped"], False)

        with self.assertRaises(TradeError) as cm:
            await post("stop_backtest", self.admin_token, data={})

        self.assertTrue(isinstance(cm.exception, TradeError))
        self.assertEqual(cm.exception.error_msg, "无法解析错误类型。原1000,错误消息为admin账号没有此功能")

        await post("stop_backtest", self.token, data={})
        info = await get("info", self.token)
        self.assertEqual(info["bt_stopped"], True)

    async def test_start_backtest(self):
        # test error handling
        principal = 1e6
        commission = 1e-4

        with self.assertRaises(BadParamsError) as cm:
            await post(
                "start_backtest",
                self.token,
                data={
                    "name": self.name,
                    "principal": principal,
                    "token": self.token,
                    "start": "2022-03-01",
                    "end": "2022-03-14",
                },
            )

        self.assertTrue(isinstance(cm.exception, BadParamsError))

        with self.assertRaises(TradeError) as cm:
            response = await post(
                "start_backtest",
                self.token,
                data={
                    "name": self.name,
                    "principal": principal,
                    "commission": commission,
                    "token": self.token,
                    "start": "hello",
                    "end": "2022-03-14",
                },
            )

            self.assertTrue(isinstance(cm, TradeError))
            self.assertEqual(
                cm.exception.error_msg,
                "parameter error: name, token, start, end, principal, commission",
            )

    async def test_assets(self):
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

        await post("stop_backtest", self.token, data={})
        response = await get("assets", self.token, start="2022-03-01", end="2022-03-14")

        exp = [
            1000039.53,
            1000514.53,
            1000594.53,
            1000084.53,
            1000124.53,
            999849.53,
            999689.53,
            1000129.53,
            1000339.53,
            1000069.53,
        ]

        np.testing.assert_array_almost_equal(exp, response["assets"].tolist(), 2)

        # issue 25
        response = await get("assets", self.token, start="2022-03-10", end="2022-03-11")
        np.testing.assert_array_equal(
            [datetime.date(2022, 3, 10), datetime.date(2022, 3, 11)], response["date"]
        )

    async def test_error_reporting(self):
        with self.assertRaises(PriceNotMeet) as cm:
            await post(
                "buy",
                self.token,
                {
                    "security": "002537.XSHE",
                    "price": 0.1,
                    "volume": 500,
                    "timeout": 0.5,
                    "order_time": "2022-03-01 10:04:00",
                    "request_id": "123456789",
                },
            )
        self.assertTrue(isinstance(cm.exception, PriceNotMeet))

        with mock.patch(
            "backtest.feed.zillionarefeed.ZillionareFeed.get_price_for_match",
            side_effect=Exception,
        ):
            with self.assertRaises(TradeError) as cm:
                await post(
                    "buy",
                    self.token,
                    {
                        "security": "002537.XSHE",
                        "price": 0.1,
                        "volume": 500,
                        "timeout": 0.5,
                        "order_time": "2022-03-01 10:04:00",
                        "request_id": "123456789",
                    },
                )
            self.assertTrue(isinstance(cm.exception, TradeError))

    async def test_save_load_backtest(self):
        r = await post(
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

        with self.assertRaises(TradeError) as cm:
            name = await post(
                "save_backtest",
                self.token,
                {
                    "name_prefix": "test",
                    "strategy_params": {"a": 120},
                    "desc": "this is a test",
                },
            )
            self.assertTrue(cm.exception.message.find("stop_backtest") != -1)

        await post("stop_backtest", self.token, {})
        name = await post(
            "save_backtest",
            self.token,
            {"name_prefix": "test", "params": {"a": 120}, "desc": "this is a test"},
        )

        state = await get("load_backtest", self.token, name=name)
        self.assertEqual(state["name"], name)
        self.assertTrue("bills" in state)
        self.assertTrue("metrics" in state)
        self.assertDictEqual(state["params"], {"a": 120})
        self.assertEqual(state["desc"], "this is a test")
