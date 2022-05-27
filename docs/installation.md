
# 以docker容器运行

docker run -d --name bt -v /host/config:/config -e PORT=3180 -p 3180:3180 backtest

上述命令中，将本地配置文件目录/host/config映射到容器中的/config目录，并且指定环境变量PORT=3180,并且将容器的3180端口映射到本地的3180端口。这里本地配置文件目录映射是必须的，否则服务器无法启动。

如果不指定PORT，则默认为7080，此时端口映射也应该相应修改为 -p 7080:7080。

!!!info
    这里-e PORT 3180的作用是，让容器内部的backtest服务器监听在3180端口，而-p 3180:3180则是让容器的3180端口映射到本地的3180端口，从而使得外部程序可以访问容器里的服务。

在/host/config目录（这是一个host主机上的目录），创建一个名为defaults.yaml的文件，其内容如下：

```yaml
redis:
  dsn: redis://redis:6379
postgres:
  dsn: postgres://zillionare:123456@postgres:5432/zillionare
  enabled: true
influxdb:
  url: http://influxdb:8086
  token: zillionare-influxdb-read-only
  org: zillionare
  bucket_name: zillionare
  enable_compress: true

server:
  path: /backtest/api/trade/v0.3/

auth:
  admin: bGZJGEZ
```

这里的`/backtest/api/trade/v0.3/`是容器里的服务器的响应路径。如果您的服务器地址为192.168.1.1，而在前面的端口映射设置为3180，则您的[traderclient](https://zillionare.github.io/traderclient)应该指向`http://192.168.1.1:3180/backtest/api/trade/v0.3/`。

注意backtest并不支持https。如果https对您而言比较重要，请在backtest server之前增加nginx一类的服务来实现。
# 本地安装运行

To install zillionare-backtest, run this command in your
terminal:

``` console
$ pip install zillionare-backtest
```

安装完成后，通过命令启动服务：
``` console
bt start
```

终止服务：
``` console
bt stop
```

查看服务状态：
``` console
bt status
```
