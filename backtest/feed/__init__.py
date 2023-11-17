"""Feed模块用以给backtest server提供撮合数据。

backtest server在进行撮合时，需要获取从下单时间起之后到当天结束时的撮合数据。backtest server本身并不提供这样的数据，它依赖data feed来提供。

backtest server本身提供了一个基于[zillionare-omicron](https://zillionare.github.io/omicron/)接口的data feed，该feeder基于分钟线数据提供撮合数据。

"""
match_data_dtype = [("frame", "O"), ("price", "f4"), ("volume", "f8")]
