# History

## 0.1.0 (2022-03-09)

* First release on PyPI.

## 0.3.1 (2022-05-31)

* if order_time <= 09:31, then use open price to match
* support xdxr
## 0.4 (2022-06-05)
* add get_assets interface

## 0.4.1 (2022-06-06)
* 在datetime（而不是date)级别上限制委托时间必须严格递增。
* 增加stop_backtest接口
* 性能改进：将计算资产(assets)的时间进行分摊，以便回测结束时，可以更快得到metrics
* the root '/' path will now display greeting message along with endpoint information in json.

## 0.4.3 （2022-06-22）
* 通过bills和positions接口取得的持仓数据包含持仓股数为零的数据，已排除(#9, #8)
* 性能增强：如果持仓表中某项股数为零，则在计算市值时直接跳过，不再查询收盘价和除权除息信息。
* bill接口中的持仓数据没有包含日期(#7)
