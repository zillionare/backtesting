class Broker:
    def __init__(self, account_name: str, cash: float, commission: float):
        self.account_name = account_name
        self.commission = commission

        self.cash = cash
        self.positions = {}
        self.trades = []
        self.orders = []
