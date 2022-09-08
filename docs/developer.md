# 构建和发布docker容器
在backtest/docker目录下，有一个build.sh文件，运行它将构建回测服务器镜像。如果要将此发布到hub.docker.com，运行以下命令：
```
./build.sh --publish
```
# 测试模式

backtest还提供了一种开发模式。这种模式下，backtest服务器将自带少量数据，方便与[trader-client](https://zillionare.github.io/trader-client/)进行联调。
```console
echo "初始化redis容器"
sudo docker run -d --name tox-redis -p 6379:6379 redis

echo "初始化influxdb容器"
sudo docker run -d -p 8086:8086 --name tox-influxdb influxdb
sleep 5
sudo docker exec -i tox-influxdb bash -c 'influx setup --username my-user --password my-password --org my-org --bucket my-bucket --token my-token --force'

sleep 1
docker run -d --name tox-bt -e MODE=TEST -e PORT=3180 -p 3180:3180 backtest

docker network create tox-bt-net
docker network connect --alias redis tox-bt-net tox-redis
docker network connect --alias influxdb tox-bt-net tox-influxdb
docker network connect --alias bt tox-bt-net tox-bt
```

提供的数据包含了天域生态、海联金汇到3月1日到3月14日止的日线和分钟线和涨跌停价格，用以撮合成交和提供收盘价数据（未复权，带复权因子）。

注意这里启动backtest容器的参数与正式运行略有不同，即多了一个`-e MODE=TEST`参数。通过这个参数，容器在启动时，将执行以下脚本：
```console
if [ $MODE = "TEST" ]; then
    if [ ! -f /root/.zillionare/backtest/config/defaults.yaml ]; then
        echo "in $MODE mode, config file not found, exit"
        echo `ls /root/.zillionare/backtest/config`
        exit
    fi

    if [ ! -f /root/.zillionare/backtest/init_db.py ]; then
        echo "init_db.py not found, exit"
        exit
    fi
    export __cfg4py_server_role__=TEST;python3 ~/.zillionare/backtest/init_db.py
    export __cfg4py_server_role__=TEST;python3 -m backtest.app start $PORT
fi
```

trader-client的单元测试中，会自动启动这个容器（需要事先将正确版本的image拉取到本地仓库，否则，自动拉取的永远是最新的版本）并进行测试。
# 排错

backtest提供了两个对账日志文件， /var/log/backtest/entrust.log和/var/log/backtest/trade.log（缺省位置，可以配置文件中的entrust和trade两项中的filename)。其中trade.log包括了买卖成交记录。可以根据这个文件和tests/data/validation.xlsx配合来进行对账。

注意两个文件的tsv文件。

如果容器构建失败，可以运行下面的命令，进入容器调试:
```
docker run -d --name tox-bt -e MODE=TEST -e PORT=3180 -p 3180:3180 backtest
```

# 文档发布
backtest遵照[ppw](https://zillionare.github.io/python-project-wizard)标准进行工程管理，因此，文档的发布是自动的。但是，您也可以手动构建和发布：

```
mike deploy -p $version
mike set-default -p $version
```
