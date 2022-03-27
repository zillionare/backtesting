import logging
import os
import pickle

from backtest.common.errors import AccountConflictError
from backtest.config import home_dir
from backtest.trade.broker import Broker

logger = logging.getLogger(__name__)


class Accounts:
    _brokers = {}

    def on_startup(self):
        state_file = os.path.join(home_dir(), "state.pkl")
        try:
            with open(state_file, "rb") as f:
                self._brokers = pickle.load(f)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.exception(e)

    def on_exit(self):
        state_file = os.path.join(home_dir(), "state.pkl")
        with open(state_file, "wb") as f:
            pickle.dump(self._brokers, f)

    def get_broker(self, token):
        return self._brokers.get(token)

    def is_valid(self, token: str):
        return token in self._brokers

    def create_account(self, token: str, name: str, capital: float, commission: float):
        """创建新账户

        为防止意外使用了他人的token，此方法会检查token,name对是否存在且相同。如果token存在，name不同，则认为是意外使用了他人的token，抛出AccountConflictError异常。

        如果之前token,name对已经存在，则调用此方法时会重置账户。
        """
        if token in self._brokers:
            broker = self._brokers[token]
            if broker.account_name != name:
                msg = f"{token[-4:]}已被{broker.name}账户使用，不能创建{name}账户"

                raise AccountConflictError(msg)

        broker = Broker(name, capital, commission)
        self._brokers[token] = broker

        return {
            "account_name": name,
            "token": token,
            "account_start_date": broker.account_start_date,
            "cash": broker.cash,
        }

    def list_accounts(self):
        return [
            {
                "account_name": broker.account_name,
                "token": token,
                "account_start_date": broker.account_start_date,
                "cash": broker.cash,
            }
            for token, broker in self._brokers.items()
        ]
