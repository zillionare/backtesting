import datetime
from typing import Dict

import arrow
from omicron.models.stock import Stock

from backtest.data.basefeed import BaseFeed
from backtest.helper import ctx, make_response
from backtest.types import EntrustError


class Broker:
    def __init__(self, account_name: str, cash: float, commission: float):

        self.account_name = account_name
        self.commission = commission

        self.cash = cash
        self.positions = {}
        self.trades = []
        self.orders = []

    async def buy(
        self,
        security: str,
        price: float,
        volume: int,
        order_time: datetime.datetime,
        timeout: int = None,
    ) -> Dict:
        """买入委托

        Args:
            security : _description_
            price : _description_
            volume : _description_
            timeout : _description_.

        Returns:
            {
                "status": 0 # 0表示成功，否则为错误码
                "msg": "blah"
                "data": {

                }
            }
        """
        # feed = ctx.feed
        # price_now = await feed.get_price(security)

        now = order_time
        buy_limit_price, _ = (
            await Stock.get_trade_price_limits(security, now, now.date())
        )[0]

        if price >= buy_limit_price:
            return make_response(EntrustError.REACH_BUY_LIMIT)
