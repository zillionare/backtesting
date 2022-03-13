import datetime
import unittest

from backtest.helper import make_response
from backtest.trade import Trade
from backtest.types import Entrust, EntrustError, EntrustSide


class HelperTest(unittest.TestCase):
    def test_make_response(self):
        response = make_response(EntrustError.REACH_BUY_LIMIT)
        self.assertDictEqual({"status": -3, "msg": "不能在涨停板上买入", "data": None}, response)

        now = datetime.datetime.now()

        order = Entrust("request_id", "security", EntrustSide.BUY, 100, 9.2, now)

        trade = Trade(order, 1.0, 100, 0.5)

        response = make_response(EntrustError.SUCCESS, trade.to_json())
        del response["data"]["tid"]

        self.assertDictEqual(
            {
                "status": 0,
                "msg": "成功委托",
                "data": {
                    "request_id": "request_id",
                    "security": "security",
                    "side": "买入",
                    "shares": 100,
                    "price": 1.0,
                    "bid_type": "市价委托",
                    "order_time": now.isoformat(),
                    "fee": 0.5,
                },
            },
            response,
        )
