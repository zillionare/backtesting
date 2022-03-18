#!/bin/sh

cp /config/defaults.yaml ~/zillionare/backtest/config/defaults.yaml
python3 -m backtest.app start $PORT
