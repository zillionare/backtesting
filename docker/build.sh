version=`poetry version | awk '{print $2}'`
wheel="/root/zillionare/zillionare_backtest-$version-py3-none-any.whl"
echo "packaging backtest version = $version"
echo "the wheel file is $wheel"
poetry build
cp ../tests/data/* rootfs/root/.zillionare/backtest/data/
cp ../dist/*$version*.whl rootfs/root/zillionare/
docker rmi backtest
docker build --build-arg version=$version --build-arg wheel=$wheel . -t backtest

if [ $# -lt 1 ]; then
    exit 0
fi

if [ $1 == "publish" ]; then
  docker tag backtest "zillionare/backtest:$version"
  docker push zillionare/backtest:$version
fi
