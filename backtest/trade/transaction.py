import datetime

from omicron.models.timeframe import TimeFrame as tf


class Transaction:
    """包括了买和卖的一次完整交易"""

    def __init__(
        self,
        sec: str,
        entry_time: datetime.datetime,
        exit_time: datetime.datetime,
        entry_price: float,
        exit_price: float,
        shares: float,
        fee: float,
    ):
        self.sec = sec
        self.entry_time = entry_time
        self.exit_time = exit_time
        self.entry_price = entry_price
        self.exit_price = exit_price
        self.shares = shares
        self.fee = fee

        self.profit = (exit_price - entry_price) * shares - fee
        self.pprofit = self.profit / (entry_price * shares)

        try:  # 如果omicron未初始化，则不计算资产暴露窗口
            self.window = tf.count_day_frames(entry_time, exit_time)
        except Exception:
            pass

    def __str__(self):
        return f"{self.sec} {self.entry_time}买入({self.entry_price}, {self.exit_time}卖出({self.exit_price}), profit {self.exit_price/self.entry_price - 1:.2%}"
