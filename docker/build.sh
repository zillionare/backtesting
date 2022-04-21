version=`poetry version | awk '{print $2}'`
echo $version
docker rmi backtest
docker build . -t backtest

if [ $1 == "publish" ]; then
  docker tag backtest zillionare/backtest:$version
  docker push zillionare/backtest:$version
fi
