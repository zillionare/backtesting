version=`poetry version | awk '{print $2}'`
echo "packaging backtest version = $version"
docker rmi backtest
export bt_version=$version;docker build . -t backtest

if [ $# -lt 1 ]; then
    exit 0
fi

if [ $1 == "publish" ]; then
  docker tag backtest "zillionare/backtest:$version"
  docker push zillionare/backtest:$version
fi
