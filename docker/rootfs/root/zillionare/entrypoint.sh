#!/bin/sh

echo
echo
echo "mode is $MODE"

if [ $MODE = "TEST" ]; then
    if [ ! -f /root/.zillionare/backtest/config/defaults.yaml ]; then
        echo "in $MODE mode, config file not found, exit"
        echo `ls /root/.zillionare/backtest/config`
        exit
    fi

    if [ ! -f /root/.zillionare/backtest/init_db.py ]; then
        echo "init_db.py not found, exit"
        exit
    fi
    export __cfg4py_server_role__=TEST;python3 ~/.zillionare/backtest/init_db.py
    export __cfg4py_server_role__=TEST;python3 -m backtest.app start $PORT
fi

# update backtest upon restart
if [ $AUTO_UPGRADE == "TRUE" ]; then
    if [ $ALLOW_PRE == "TRUE" ]; then
        pip3 install --pre zillionare-backtest --default-timeout=300 -i https://pypi.tuna.tsinghua.edu.cn/simple
    else
        pip3 install zillionare-backtest --default-timeout=300 -i https://pypi.tuna.tsinghua.edu.cn/simple
    fi
fi

echo "port passed through envar: $PORT" > /var/log/backtest/backtest.log

python3 -m backtest.app start $PORT
