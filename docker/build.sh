version=`poetry version | awk '{print $2}'`
backtest_whl="/root/zillionare/zillionare_backtest-$version-py3-none-any.whl"
omicron_whl="/root/zillionare/zillionare_omicron-2.0.0a77-py3-none-any.whl"
echo "packaging backtest version = $version"
echo "the backtest wheel file is $backtest_whl"
poetry build
cp ../tests/data/* rootfs/root/.zillionare/backtest/data/
cp ../dist/*$version*.whl rootfs/root/zillionare/
sudo docker rmi backtest
sudo docker build --build-arg version=$version --build-arg backtest=$backtest_whl --build-arg omicron=$omicron_whl . -t backtest

# test the image
sudo docker run -d --name bt -v ~/zillionare/backtest/config:/config -p 7080:7080 -e MODE=TEST backtest
sleep 20
response=`curl -sSf http://localhost:7080/`

if [[ "$response" == *"greetings"* ]]; then
    echo "backtest image is good"
    if [ $# -lt 1 ]; then
        exit 0
    fi

    if [ $1 == "publish" ]; then
        echo "push image to docker hub"
        sudo docker tag backtest "zillionare/backtest:$version"
        sudo docker push zillionare/backtest:$version
    fi
else
    echo "docker image failed"
    exit 1
fi
