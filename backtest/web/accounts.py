from functools import lru_cache

import cfg4py

from backtest.trade.broker import Broker


class Accounts:
    _brokers = {}

    def __init__(self):
        cfg = cfg4py.get_instance()
        self._accounts = cfg.accounts

    @lru_cache(maxsize=32)
    def is_valid(self, token):
        for item in self._accounts:
            if item["token"] == token:
                return True
        return False

    def _get_account_info(self, token):
        for item in self._accounts:
            if item["token"] == token:
                return item["name"], item["cash"], item["commission"]

        return None

    def get_broker(self, token):
        if self._brokers.get(token) is None:
            name, cash, commission = self._get_account_info(token)
            broker = Broker(name, cash, commission)
            self._brokers[token] = broker
            return broker
        else:
            return self._brokers[token]
