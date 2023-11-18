import datetime
import uuid
from typing import Union

from omicron.core.backtestlog import BacktestLogger

from backtest.trade.datatypes import EntrustSide
from backtest.trade.transaction import Transaction

logger = BacktestLogger.getLogger(__name__)


class Trade:
    """Trade对象代表了一笔已成功完成的委托。一个委托可能对应多个Trade，特别是当卖出的时候"""

    def __init__(
        self,
        eid: str,
        security: str,
        price: float,
        shares: Union[float, int],
        fee: float,
        side: EntrustSide,
        time: datetime.datetime,
    ):
        """Trade对象代表了一笔已成功的委托（即已完成的交易）

        Args:
            eid: 对应的委托号
            security: 证券代码
            price: 交易价格
            shares: 交易数量
            fee: 交易手续费
            time: 交易时间
        """
        self.eid = eid
        self.tid = str(uuid.uuid4())
        self.security = security

        self.fee = fee
        self.price = price
        self.shares = shares
        self.time = time

        self.side = side

        # only for buying trade
        if side in (EntrustSide.BUY, EntrustSide.XDXR):
            self._unsell = shares
            self._unamortized_fee = fee
            self.closed = False

        if side == EntrustSide.XDXR:
            logger.info("XDXR entrust: %s", self, date=time)

    def __str__(self):
        return f"证券代码: {self.security}\n成交方向: {self.side}\n成交均价: {self.price}\n数量: {self.shares}\n手续费: {self.fee}\n委托号: {self.eid}\n成交号: {self.tid}\n成交时间: {self.time}\n"

    def to_dict(self) -> dict:
        """将Trade对象转换为字典格式。

        Returns:
            Dict: 返回值，其key为

            - tid: 交易号
            - eid: 委托号
            - security: 证券代码
            - price: 交易价格
            - filled: 居交数量
            - trade_fees: 交易手续费
            - order_side: 交易方向
            - time: 交易时间

        """
        return {
            "tid": str(self.tid),
            "eid": str(self.eid),
            "security": self.security,
            "order_side": str(self.side),
            "price": self.price,
            "filled": self.shares,
            "time": self.time.isoformat(),
            "trade_fees": self.fee,
        }

    def sell(
        self, shares: float, price: float, fee: float, close_time: datetime.datetime
    ):
        """从当前未售出股中售出。

        计算时将根据售出的股数，分摊买入和卖的交易成本。返回未售出的股份和未分摊的成本。

        Args:
            shares: 待出售股数
            price: 出售价格
            fee: 交易手续费
            close_time: 成交日期
        """
        assert self.side in (EntrustSide.BUY, EntrustSide.XDXR)

        if not self.closed:
            sec = self.security
            assert self._unsell > 0, str(self) + "状态错误，无法售出，请检查代码"

            sellable = min(shares, self._unsell)

            # 计算本次交易的收益，并分摊交易成本
            amortized_buy_fee = self.fee * sellable / self.shares
            amortized_sell_fee = fee * sellable / shares

            self._unsell -= sellable
            self._unamortized_fee -= amortized_buy_fee

            if self._unsell <= 1e-5:
                logger.debug(
                    "交易%s (%s)已close.", self.security, self.tid, date=close_time
                )
                self.closed = True

            trade = Trade(
                self.eid,
                sec,
                price,
                sellable,
                amortized_sell_fee,
                EntrustSide.SELL,
                close_time,
            )

            tx = Transaction(
                sec,
                self.time,
                close_time,
                self.price,
                price,
                sellable,
                amortized_buy_fee + amortized_sell_fee,
            )

            return shares - sellable, fee - amortized_sell_fee, trade, tx
