version=`poetry version | awk '{print $2}'`
echo "packaging backtest version = $version"
docker rmi backtest
docker build --build-arg version=$version . -t backtest

if [ $# -lt 1 ]; then
    exit 0
fi

if [ $1 == "publish" ]; then
  docker tag backtest "zillionare/backtest:$version"
  docker push zillionare/backtest:$version
fi
