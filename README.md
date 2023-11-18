[![Version](http://img.shields.io/pypi/v/zillionare-backtest?color=brightgreen)](https://pypi.python.org/pypi/zillionare-backtest)
[![CI Status](https://github.com/zillionare/backtesting/actions/workflows/release.yml/badge.svg)](https://github.com/zillionare/backtesting)
[![Code Coverage](https://img.shields.io/codecov/c/github/zillionare/backtesting)](https://app.codecov.io/gh/zillionare/backtesting)
[![Downloads](https://pepy.tech/badge/zillionare-backtest)](https://pepy.tech/project/zillionare-backtest)
[![License](https://img.shields.io/badge/License-MIT.svg)](https://opensource.org/licenses/MIT)
[![Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# zillionare-backtest

zillionare-backtest是大富翁的回测服务器，它跟[zillionare-omega](https://zillionare.github.io/omega/), [zillionare-omicron](https://zillionare.github.io/omicron), [zillionare-alpha](https://zillionare.github.io/alpha), [zillionare-trader-client](https://zillionare.github.io/traderclient)共同构成回测框架。

zillionare-backtest的功能是提供账户管理、交易撮合和策略评估。zillionare-backtest使用omicron来提供撮合数据，但您也可以自写开发撮合数据的提供器[^1]。

与普通的回测框架不同，大富翁回测框架并非是侵入式的。在您的策略中，只需要接入我们的trader-client,并在策略发出交易信号时，向backtest server发出对应的交易指令，即可完成回测。当回测完成，转向实盘时，不需要修改策略代码，仅需要指回测服务器url指向[zillionare-trader-server](https://zillionare.github.io/traderserver/)即可。zillionare-backtest与zillionare-trader-server的API绝大多数地方是完全兼容的。

这种设计意味着，您的策略可以不使用大富翁数据框架，甚至可以不使用zillionare-trader-client（您可以自定义一套接口并实现，使之能同时适配您的交易接口和backtest接口）。因此，您的策略可以在任何时候，切换到最适合的量化框架。

# 功能
## 账户管理
当您开始回测时，先通过[start_backtest][backtest.web.interfaces.start_backtest]来创建一个账户。在知道该账户的`name`与`token`的情况下，您可以在随后通过[delete_accounts][backtest.web.interfaces.delete_accounts]来删除账户。

在回测完成时，请记得调用stop_backtest。

在回测完成时，stop_backtest会将资产表更新到回测结束日（否则，只更新到最后一次交易当天，因为服务器完全由客户端来驱动，自己没有时间概念）。但并不会对当前持仓进行卖出操作，原因是：

1. 卖出操作将修改transactions表。而新增的transaction并不是策略触发的
   
2. 不利于评估策略的真实情况。如果在回测期出现仅有一笔真实交易，其它都是被终末强平的话，那么此次回测实际上可能在时间上、或者策略周期上没有选好。如果回测系统进行强平，就可能掩盖这种事实。
   
3. 在回测终末期，可能存在股票因跌停而无法卖出的情况；或者股票停牌中，无法卖出。这些情况下，模拟卖出也有难度。

## 交易撮合

您可以通过[buy][backtest.web.interfaces.buy], [market_buy][backtest.web.interfaces.market_buy], [sell][backtest.web.interfaces.sell], [market_sell][backtest.web.interfaces.market_sell]和[sell_percent][backtest.web.interfaces.sell_percent]来进行交易。

## 状态跟踪

您可以通过[info][backtest.web.interfaces.info]来查看账户的基本信息，比如当前总资产、持仓、本金、盈利等。您还可以通过[positions][backtest.web.interfaces.positions]、[bills][backtest.web.interfaces.bills]来查看账户的持仓、交易历史记录。

## 策略评估

[metrics][backtest.web.interfaces.metrics]方法将返回策略的各项指标，比如sharpe, sortino, calmar, win rate, max drawdown等。您还可以传入一个参考标的，backtest将对参考标的也同样计算上述指标。

# 关键概念

## 复权处理
您的策略在发出买卖信号时，应该使用与`order_time`一致的现价，而不是任何复权价。如果您的持仓在持有期间，发生了分红送股，回测服务器会自动将分红送股转换成股数加到您的持仓中。当您最终清空持仓时，可以通过`bills`接口查询到分红送股的成交情况（记录为XDXR类型的委托）。

## 撮合机制
在撮合时，backtest首先从data feeder中获取`order_time`以后（含）的行情数据。接下来去掉处在涨跌停中的那些bar（如果是委买，则去掉已处在涨停期间的bar，反之亦然）。在剩下的bar中，backtest会选择价格低于委托价的那些bar（如果是委卖，则选择价格高于委托价的那些bar）,依顺序匹配委托量，直到委托量全部被匹配为止。最后，backtest将匹配到的bar的量和价格进行加权平均，得到成交均价。

当backtest使用zillionare-feed来提供撮合数据时，由于缺少盘口数据，zillionare-feed使用分钟级行情数据中的`close`价格和`volume`来进行撮合。因此，可能出现某一分钟的最高价或者最低价可能满足过您的委托价要求，但backtest并未成交撮合的情况。我们这样设计，主要考虑到当股价达到最高或者最低点时，当时的成交量不足以满足委托量。现在backtest的设计，可能策略的鲁棒性更好。

作为一个例外，如果委托时的`order_time`为9:31分之前，backtest将会使用9:31分钟线的开盘价，而不是9:31分的收盘价来进行撮合，以满足部分策略需要以**次日开盘价**买入的需求。

另外，您也应该注意到，zillionare-feed使用分钟线来替代了盘口数据，尽管在绝大多数情形下，这样做不会有什么影响，但两者毕竟是不同的。一般来说，成交量肯定小于盘口的委买委卖量。因此，在回测中出现买卖委托量不足的情况时，对应的实盘则不一定出现。在这种情况下，可以适当调低策略的本金设置。另外一个差异是，分钟成交价必然不等同于盘口成交价，因此会引入一定的误差。不过长期来看，这种误差应该是零均值的，因此对绝大多数策略不会产生实质影响。

!!!info
    了解backtest的撮合机制后，您应该已经明白，正确设定策略的本金(`principal`)会使得回测的系统误差更小。

## 委买委卖
委买时，委买量必须是100股的整数倍。这个限制与实盘是一致的。同样，您的券商对委卖交易也做了限制，但回测服务器并未对此进行限制。经评估，去掉这个限制并不会对策略的有效性产生任何影响，但会简化策略的编写。

## 保存和查询回测状态
如果某次回测数据比较好，您可以`save_backtest`接口来保存回测状态（transaction, metrics, bills, positions），以及策略参数和描述。通过调用`load_backtest`来获取

## 停牌处理
如果某支持仓股当前停牌，在计算持仓市值时，系统会使用停牌前的收盘价来计算市值。为性能优化考验，如果一支股票停牌时间超过500个交易日，则系统将放弃继续向前搜索停牌前的收盘价，改用买入时的成交均价来代替。这种情况应该相当罕见。
# 版本历史
关于版本历史，请查阅[版本历史](history)
# Credits

Zillionare-backtest项目是通过[Python Project Wizard](zillionare.github.io/python-project-wizard)创建的。


[^1]:此功能在0.5.x版本中尚不可用。
