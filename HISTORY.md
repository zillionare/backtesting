# History

!!! Info
    `(#{number})` means an issue of this project. You may check details of the issue by visiting https://github.com/zillionare/backtesting/issues/_{number}_

## 0.4.12
*[#27](https://github.com/zillionare/backtesting/issues/27) 采用omicron 2.0.0.a41,修复此问题。
## 0.4.11
*[#26](https://github.com/zillionare/backtesting/issues/26) 容器中使用了错误的numpy版本，导致了本错误。
## 0.4.10
* [#22](https://github.com/zillionare/backtesting/issues/22) bills接口返回的'tx'字段中的pprofit计算错误
* [#25](https://github.com/zillionare/backtesting/issues/25) get_assets中，如果传入了start参数，则会抛出"operands could not be broadcast"异常。
## 0.4.9
* 引入omicron 2.0.0.a37
* [#18](https://github.com/zillionare/backtesting/issues/18) 将容器中的/var/log/backtest目录映射到宿主机。
* [#19](https://github.com/zillionare/backtesting/issues/19) 分红送股额显示异常
* [#20](https://github.com/zillionare/backtesting/issues/20) 获取600361在2022-08-11的行情数据失败。
## 0.4.8
* (#17) 当持仓股存在除权除息时，会导致新增的类型为EntrustSide.XDXR的Trader,其price字段为数组，从而导致metrics计算失误（当然也会引起其它错误）。
* (#18) 允许将日志文件目录/var/log/backtest映射到host主机上的目录。
## 0.4.7
* (#16) 当持仓中有停牌的股票时，计算assets时，因取不到指定期的价格数据，导致计算错误。
## 0.4.6
* (#15) 如果持仓中有长期停牌的股票，可能导致抛出异常。

## 0.4.5
* (#14) 如果持仓不为空，但持仓股处在停牌期，此时其它股票也不能交易（买入或者卖出）。

## 0.4.4
* 在卖出时，允许委卖股为非整数
* (#12) 修复了在没有进行过交易之前就通过get_assets查询资产时报错
* (#13) 修复了分红和送转股无法卖出的问题

## 0.4.3 （2022-06-22）
* (#9, #8) 通过bills和positions接口取得的持仓数据包含持仓股数为零的数据，已排除
* 性能增强：如果持仓表中某项股数为零，则在计算市值时直接跳过，不再查询收盘价和除权除息信息。
* bill接口中的持仓数据没有包含日期(#7)

## 0.4.1 (2022-06-06)
* 在datetime（而不是date)级别上限制委托时间必须严格递增。
* 增加stop_backtest接口
* 性能改进：将计算资产(assets)的时间进行分摊，以便回测结束时，可以更快得到metrics
* the root '/' path will now display greeting message along with endpoint information in json.

## 0.4 (2022-06-05)
* add get_assets interface

## 0.3.1 (2022-05-31)

* if order_time <= 09:31, then use open price to match
* support xdxr

## 0.1.0 (2022-03-09)

* First release on PyPI.
