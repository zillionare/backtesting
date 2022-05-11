import datetime
import logging
import os
import pickle

import cfg4py

from backtest.common.errors import AccountError
from backtest.config import home_dir
from backtest.trade.broker import Broker

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class Accounts:
    _brokers = {}

    def on_startup(self):
        token = cfg.auth.admin
        self._brokers[token] = Broker("admin", 0, 0.0)

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

    def is_admin(self, token: str):
        cfg = cfg4py.get_instance()
        return token == cfg.auth.admin

    def create_account(
        self,
        name: str,
        token: str,
        capital: float,
        commission: float,
        start: datetime.date = None,
        end: datetime.date = None,
    ):
        """创建新账户

        Args:
            name (str): 账户/策略名称
            token (str): 账户token
            capital (float): 账户起始资金
            commission (float): 账户手续费
            start (datetime.date, optional): 回测开始日期，如果是模拟盘，则可为空
            end (datetime.date, optional): 回测结束日期，如果是模拟盘，则可为空
        """
        if token in self._brokers:
            msg = f"账户{name}:{token}已经存在，不能重复创建。"
            raise AccountError(msg)

        for broker in self._brokers.values():
            if broker.account_name == name:
                msg = f"账户{name}:{token}已经存在，不能重复创建。"
                raise AccountError(msg)

        broker = Broker(name, capital, commission, start, end)
        self._brokers[token] = broker

        logger.info("新建账户:%s, %s", name, token)
        return {
            "account_name": name,
            "token": token,
            "account_start_date": broker.account_start_date,
            "capital": broker.capital,
        }

    def list_accounts(self, mode: str):
        if mode != "all":
            filtered = {
                token: broker
                for token, broker in self._brokers.items()
                if broker.mode == mode and broker.account_name != "admin"
            }
        else:
            filtered = {
                token: broker
                for token, broker in self._brokers.items()
                if broker.account_name != "admin"
            }

        return [
            {
                "account_name": broker.account_name,
                "token": token,
                "account_start_date": broker.account_start_date,
                "capital": broker.capital,
            }
            for token, broker in filtered.items()
        ]

    def delete_accounts(self, account_to_delete: str = None):
        if account_to_delete is None:
            self._brokers = {}
            self._brokers[cfg.auth.admin] = Broker("admin", 0, 0.0)
            return 0
        else:
            for token, broker in self._brokers.items():
                if broker.account_name == account_to_delete:
                    del self._brokers[token]
                    logger.info("账户:%s已删除", account_to_delete)

                    return len(self._brokers) - 1
            else:
                logger.warning("账户%s不存在", account_to_delete)
                return len(self._brokers)
