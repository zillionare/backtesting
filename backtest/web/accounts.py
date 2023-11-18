"""简易账户管理

提供了创建账户、查询账户、删除账户和状态持久化实现。
"""
import datetime
import json
import os
import pickle
import uuid
from typing import Any, Dict, Optional, Union

import cfg4py
from coretypes.errors.trade import AccountConflictError, BadParamsError, TradeError
from omicron.core.backtestlog import BacktestLogger

from backtest.config import home_dir
from backtest.trade.broker import Broker

logger = BacktestLogger.getLogger(__name__)
cfg = cfg4py.get_instance()

admin_start_end_dt = datetime.date(2099, 9, 9)

backtest_index_file = os.path.join(home_dir(), "backtest.index.json")


class Accounts:
    _brokers: Dict[str, Broker] = {}
    _state_index = {}

    def on_startup(self):
        token = cfg.auth.admin
        self._brokers[token] = Broker(
            "admin", 0, 0.0, admin_start_end_dt, admin_start_end_dt
        )

        try:
            with open(backtest_index_file, "r") as f:
                self._state_index = json.load(f)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.exception(e)

    def on_exit(self):
        with open(backtest_index_file, "w") as f:
            json.dump(self._state_index, f)

    def get_broker(self, token: str) -> Union[None, Broker]:
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
        principal: float,
        commission: float,
        start: datetime.date,
        end: datetime.date,
    ):
        """创建新账户

        一个账户由`name`和`token`的组合惟一确定。如果前述组合已经存在，则创建失败。

        Args:
            name (str): 账户/策略名称
            token (str): 账户token
            principal (float): 账户起始资金
            commission (float): 账户手续费
            start (datetime.date): 回测开始日期，如果是模拟盘，则可为空
            end (datetime.date): 回测结束日期，如果是模拟盘，则可为空
        """
        if token in self._brokers:
            msg = f"账户{name}:{token}已经存在，不能重复创建。"
            raise AccountConflictError(msg, with_stack=True)

        for broker in self._brokers.values():
            if broker.account_name == name:
                msg = f"账户{name}:{token}已经存在，不能重复创建。"
                raise AccountConflictError(msg, with_stack=True)

        broker = Broker(name, principal, commission, start, end)
        self._brokers[token] = broker

        logger.info("新建账户:%s, %s", name, token)
        return {
            "account_name": name,
            "token": token,
            "account_start_date": broker.bt_start,
            "principal": broker.principal,
        }

    def list_accounts(self):
        filtered = {
            token: broker
            for token, broker in self._brokers.items()
            if broker.account_name != "admin"
        }

        return [
            {
                "account_name": broker.account_name,
                "token": token,
                "account_start_date": broker.bt_start,
                "principal": broker.principal,
            }
            for token, broker in filtered.items()
        ]

    def delete_accounts(self, account_to_delete: Optional[str] = None):
        if account_to_delete is None:
            self._brokers = {}
            self._brokers[cfg.auth.admin] = Broker(
                "admin", 0, 0.0, admin_start_end_dt, admin_start_end_dt
            )
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

    async def save_backtest(
        self,
        name_prefix: str,
        strategy_params: Optional[dict],
        token: str,
        baseline: str = "399300.XSHE",
        desc: str = "",
    ) -> str:
        """保存回测数据及参数

        Args:
            name_prefix: 策略名前缀
            strategy_params: 策略参数
        Returns:
            策略名称
        """
        while True:
            name = name_prefix + uuid.uuid4().hex[:4]
            if name not in self._state_index:
                break

        state_file = os.path.join(home_dir(), name + ".pkl")
        broker = self.get_broker(token)
        if broker is None:
            raise BadParamsError(f"cannot found backtest represented by {token}")

        if not broker._bt_stopped:
            raise TradeError("call `stop_backtest` first!")

        with open(state_file, "wb") as f:
            pickle.dump(
                {
                    "name": name,
                    "bills": broker.bills(),
                    "metrics": await broker.metrics(baseline=baseline),
                    "params": strategy_params or {},
                    "desc": desc,
                },
                f,
            )

        self._state_index[name] = {"token": token}

        with open(backtest_index_file, "w") as f:
            json.dump(self._state_index, f)

        return name

    def load_backtest(self, name: str, token: str) -> Any:
        """从磁盘加载已保存的回测

        Args:
            name: name of the backtest used to save the backtest
            token: token used to validate the request
        """
        if name not in self._state_index:
            raise BadParamsError(f"{name} not exists")

        if token != self._state_index[name]["token"]:
            raise BadParamsError("token is incorrect")

        state_file = os.path.join(home_dir(), name + ".pkl")
        with open(state_file, "rb") as f:
            return pickle.load(f)
