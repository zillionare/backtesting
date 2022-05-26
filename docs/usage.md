# 基本原理

与普通的回测框架不同，zillionare-backtest通过将自己仿真成为交易柜台来进行回测。backtest使用C/S模式。策略端通过调用client sdk，将交易指令发送给backtest server。bactest server完成撮合、仓位和市值更新等操作，并计算收益率。

要使用backtest服务器，必须配置撮合数据源。zillionare-backtest支持两种数据源，一种是zillionare feed,一种是文件数据源。要得到最接近实盘的效果，最好提供分钟级别的数据。

## 客户端安装和使用

client-sdk安装：
```
pip install zillionare-trader-client
```

使用示例：
```
from traderclient import TraderClient

client = TraderClient(url, account, token)
client.buy(symbol, price, amount)
```
详细使用请参考[客户端使用](https://zillionare-trader-client.readthedocs.io/）

## 服务器配置

服务器配置文件使用yaml格式。需要配置项主要有：

### server path
缺省为/backtest/api/trade/v0.2/，可以通过修改以下配置项来修改服务器路径：
```
server:
    path: /backtest/api/trade/v0.2/
```

### 账户配置
```
accounts:
  - name: "aaron"
    cash: 1000000
    commission: 0.0001
    token: "abcd"
```
backtest支持多账户同时进行回测。每个账户必须配置不同的token（账户仅仅通过token来进行区分）。另外，还需要配置账户名，账户初始资金，手续费率。

backtest服务器并不检查用户名。

backtest服务器不支持复杂的手续费设定。如果你的策略对手续费敏感，应该使用较大的资金量和适当的手续费率来模拟。在量化中，过分强调每笔交易的最低手费门槛是没有太多意义的，因为量化资金规模不会太小。

### 数据源配置
``` yaml
datasource:
  feeder: omicron
  omicron:
    redis:
      dsn: redis://localhost:6379
    postgres:
      dsn: postgres://zillionare:123456@localhost:5432/zillionare
      enabled: true
    influxdb:
      url: http://localhost:8086
      token: my-token
      org: my-org
      bucket_name: my-bucket
      enable_compress: true
```
