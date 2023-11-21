if [ $IS_GITHUB ]; then
    echo "runs on github, skipping start_service.sh"
    exit 0
fi

echo "本地测试环境，将初始化redis"

export TZ=Asia/Shanghai

echo "初始化redis容器"
sudo docker run -d --name tox-redis -p 6379:6379 redis

echo "初始化influxdb容器"
sudo docker run -d -p 8086:8086 --name tox-influxdb influxdb
sleep 5
sudo docker exec -i tox-influxdb bash -c 'influx setup --username my-user --password my-password --org my-org --bucket my-bucket --token my-token --force'

sleep 3

sudo docker run -d --name tox-bt -e MODE=TEST -e PORT=3180 -p 3180:3180 --env-file ./.env backtest

sudo docker network create tox-bt-net
sudo docker network connect --alias redis tox-bt-net tox-redis
sudo docker network connect --alias influxdb tox-bt-net tox-influxdb
sudo docker network connect --alias bt tox-bt-net tox-bt
