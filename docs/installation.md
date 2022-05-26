
# 以docker容器运行

docker run -d --name bt -v /host/config:/config -e PORT=3180 -p 3180:3180 backtest

上述命令中，将本地配置文件目录/host/config映射到容器中的/config目录，并且指定环境变量PORT=3180,并且将容器的3180端口映射到本地的3180端口。这里本地配置文件目录映射是必须的，否则服务器无法启动。

如果不指定PORT，则默认为7080，此时端口映射也应该相应修改为 -p 7080:7080。

在/host/config目录（这是一个host主机上的目录），创建一个名为defaults.yaml的文件，其内容如下：

```yaml
redis:
  dsn: redis://redis.z:56379
postgres:
  dsn: postgres://zillionare:123456@postgres.z:55432/zillionare
  enabled: true
influxdb:
  url: http://influx.z:58086
  token: zillionare-influxdb-read-only
  org: zillionare
  bucket_name: zillionare
  enable_compress: true

server:
  path: /backtest/api/trade/v0.2/
accounts:
  - name: "aaron"
    cash: 1_000_000
    commission: 0.0001
    token: "abcd"
```

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
