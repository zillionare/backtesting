import unittest

from backtest.helper import make_response
from backtest.types import EntrustError


class HelperTest(unittest.TestCase):
    def test_make_response(self):
        response = make_response(EntrustError.REACH_BUY_LIMIT)
        self.assertDictEqual({"status": -3, "msg": "不能在涨停板上买入", "data": {}}, response)
