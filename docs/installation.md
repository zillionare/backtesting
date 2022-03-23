# Installation

## docker

docker run -d --name bt -v /host/config:/config -e port=3180 -p 3180:3180 backtest

in the /host/config directory, create a file called defaults.yaml with the following content:
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
  port: 7080
accounts:
  - name: "aaron"
    cash: 1_000_000
    commission: 0.0001
    token: "abcd"
```

## Stable release

To install zillionare-backtest, run this command in your
terminal:

``` console
$ pip install backtest
```

This is the preferred method to install zillionare-backtest, as it will always install the most recent stable release.

If you don't have [pip][] installed, this [Python installation guide][]
can guide you through the process.

## From source

The source for zillionare-backtest can be downloaded from
the [Github repo][].

You can either clone the public repository:

``` console
$ git clone git://github.com/zillionare/backtest
```

Or download the [tarball][]:

``` console
$ curl -OJL https://github.com/zillionare/backtest/tarball/master
```

Once you have a copy of the source, you can install it with:

``` console
$ pip install .
```

  [pip]: https://pip.pypa.io
  [Python installation guide]: http://docs.python-guide.org/en/latest/starting/installation/
  [Github repo]: https://github.com/%7B%7B%20cookiecutter.github_username%20%7D%7D/%7B%7B%20cookiecutter.project_slug%20%7D%7D
  [tarball]: https://github.com/%7B%7B%20cookiecutter.github_username%20%7D%7D/%7B%7B%20cookiecutter.project_slug%20%7D%7D/tarball/master
