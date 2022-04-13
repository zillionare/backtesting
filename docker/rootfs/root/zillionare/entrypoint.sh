#!/bin/sh

if [ ! -f /config/defaults.yaml ]; then
    echo "config file not found, exiting"
    return
fi

pip3 install --pre zillionare-backtest --default-timeout=300 -i https://pypi.org/simple
echo "port passed through envar: $PORT" > /var/log/backtest/backtest.log

cp /config/defaults.yaml ~/zillionare/backtest/config/defaults.yaml
python3 -m backtest.app start $PORT
