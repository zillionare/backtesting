# zillionare-backtest


<p align="center">
<a href="https://pypi.python.org/pypi/backtest">
    <img src="https://img.shields.io/pypi/v/backtest.svg"
        alt = "Release Status">
</a>
</p>

zillionare-backtest是大富翁的回测服务器，它跟[zillionare-omega](https://zillionare.github.io/omega/), [zillionare-omicron](https://zillionare.github.io/omicron), [zillionare-alpha](https://zillionare.github.io/alpha), [zillionare-trader-client](https://zillionare.github.io/traderclient)共同构成回测框架。

zillionare-backtest的功能是提供账户管理、交易撮合和策略评估。zillionare-backtest使用omicron来提供撮合数据，但您也可以自写开发撮合数据的提供器。

与普通的回测框架不同，大富翁回测框架并非是侵入式的。在您的策略中，只需要接入我们的trader-client,并在策略发出交易信号时，向backtest server发出对应的交易指令，即可完成回测。当回测完成，转向实盘时，不需要修改策略代码，仅需要指回测服务器url指向[zillionare-trader-server](https://zillionare.github.io/traderserver/)即可。zillionare-backtest与zillionare-trader-server的API绝大多数地方是完全兼容的。

这种设计意味着，您的策略可以不使用大富翁数据框架，甚至可以不使用zillionare-trader-client（您可以自定义一套接口并实现，使之既适配您的交易接口和backtest接口）。因此，您的策略可以在任何时候，切换到最适合的量化框架。

## 账户管理
当您开始回测时，先通过[start_backtest][backtest.web.interfaces.start_backtest]来创建一个账户。在知道该账户的`name`与`token`的情况下，您可以在随后通过[delete_accounts][backtest.web.interfaces.delete_accounts]来删除账户。

## 交易撮合

您可以通过[buy][backtest.web.interfaces.buy], [market_buy][backtest.web.interfaces.market_buy], [sell][backtest.web.interfaces.sell], [market_sell][backtest.web.interfaces.market_sell]和[sell_percent][backtest.web.interfaces.sell_percent]来进行交易。

## 状态跟踪

您可以通过[info][backtest.web.interfaces.info]来查看账户的基本信息，比如当前总资产、持仓、本金、盈利等。您还可以通过[positions][backtest.web.interfaces.positions]、[bills][backtest.web.interfaces.bills]来查看账户的持仓、交易历史记录
## 策略评估

[metrics][backtest.web.interfaces.metrics]方法将返回策略的各项指标，比如sharpe, sortino, calmar, win rate, max drawdown等。您还可以传入一个参考标的，backtest将对参考标的也同样计算上述指标。
## Credits

This package was [Python Project Wizard](zillionare.github.io/python-project-wizard) project template.
