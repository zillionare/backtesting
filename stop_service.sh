if [ $IS_GITHUB ]; then
    echo "runs on github, skipping stop_service.sh"
    exit 0
fi

echo "将移除start_service脚本启动的本地环境中的redis, minio和influxdb容器!"

sudo docker rm -f tox-redis
sudo docker rm -f tox-influxdb
sudo docker rm -f tox-bt

sudo docker ps -a
sudo docker network rm tox-bt-net
