import datetime
import unittest

import numpy as np

from backtest.common.helper import jsonify, make_response
from backtest.trade.trade import Trade
from backtest.trade.types import Entrust, EntrustError, EntrustSide


class HelperTest(unittest.TestCase):
    def test_make_response(self):
        response = make_response(EntrustError.REACH_BUY_LIMIT)
        self.assertDictEqual({"status": -3, "msg": "不能在涨停板上买入", "data": None}, response)

        now = datetime.datetime.now()

        order = Entrust("security", EntrustSide.BUY, 100, 9.2, now)

        trade = Trade(order.eid, order.security, 1.0, 100, 0.5, EntrustSide.BUY, now)

        response = make_response(EntrustError.SUCCESS, trade.to_dict())
        del response["data"]["tid"]

        self.assertDictEqual(
            {
                "status": 0,
                "msg": "成功委托",
                "data": {
                    "eid": order.eid,
                    "security": "security",
                    "order_side": "买入",
                    "volume": 100,
                    "price": 1.0,
                    "time": now.isoformat(),
                    "trade_fees": 0.5,
                },
            },
            response,
        )

    def test_obj_to_dict(self):
        obj = {
            "numpy": np.array([0.1, 0.2, 0.3]),
            "time": datetime.datetime(2020, 1, 1, 0, 0, 0),
            "list": [1, 2, 3],
            "dict": {"a": 1, "b": 2},
            "str": "hello",
            "bool": False,
        }

        print(jsonify(obj))
