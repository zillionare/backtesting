docker rmi backtest
docker build . -t backtest

if [ $1 == "publish" ]; then
  docker tag backtest zillionare/backtest:latest
  docker push zillionare/backtest:latest
fi
