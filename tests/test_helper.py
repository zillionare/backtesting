import datetime
import unittest

from backtest.common.helper import make_response
from backtest.trade.trade import Trade
from backtest.trade.types import Entrust, EntrustError, EntrustSide


class HelperTest(unittest.TestCase):
    def test_make_response(self):
        response = make_response(EntrustError.REACH_BUY_LIMIT)
        self.assertDictEqual({"status": -3, "msg": "不能在涨停板上买入", "data": None}, response)

        now = datetime.datetime.now()

        order = Entrust("security", EntrustSide.BUY, 100, 9.2, now)

        trade = Trade(order.eid, order.security, 1.0, 100, 0.5, EntrustSide.BUY, now)

        response = make_response(EntrustError.SUCCESS, trade.to_json())
        del response["data"]["tid"]

        self.assertDictEqual(
            {
                "status": 0,
                "msg": "成功委托",
                "data": {
                    "eid": order.eid,
                    "security": "security",
                    "side": "买入",
                    "shares": 100,
                    "price": 1.0,
                    "time": now.isoformat(),
                    "fee": 0.5,
                },
            },
            response,
        )
