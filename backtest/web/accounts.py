from backtest.trade.broker import Broker


class Accounts:
    _brokers = {}

    def get_broker(self, token):
        return self._brokers.get(token)

    def is_valid(self, token: str):
        return token in self._brokers

    def create_account(self, token: str, name: str, capital: float, commission: float):
        if token not in self._brokers:
            broker = Broker(name, capital, commission)
            self._brokers[token] = broker
        else:
            broker = self._brokers[token]

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
