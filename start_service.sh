if [ $IS_GITHUB ]; then
    echo "runs on github, skipping start_service.sh"
    exit 0
fi

echo "本地测试环境，将初始化redis"

export TZ=Asia/Shanghai
sudo -E apt-get update

echo "初始化redis容器"
sudo docker run -d --name tox-redis -p 6379:6379 redis

sleep 3
