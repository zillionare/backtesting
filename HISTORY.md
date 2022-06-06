# History

## 0.1.0 (2022-03-09)

* First release on PyPI.

## 0.3.1 (2022-05-31)

* if order_time <= 09:31, then use open price to match
* support xdxr
## 0.4 (2022-06-05)
* add get_assets interface

## 0.4.1
* 在datetime（而不是date)级别上限制委托时间必须严格递增。
* 增加stop_backtest接口
* 性能改进：将计算资产(assets)的时间进行分摊，以便回测结束时，可以更快得到metrics
