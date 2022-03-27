#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging
import os
import sys
from os import path

import cfg4py

logger = logging.getLogger(__name__)


def get_config_dir():
    server_role = os.environ.get(cfg4py.envar)

    if server_role == "DEV":
        _dir = path.normpath(path.join(path.dirname(__file__), "../config"))
    elif server_role == "TEST":
        _dir = path.expanduser("~/.zillionare/backtest/config")
    else:
        _dir = path.expanduser("~/zillionare/backtest/config")

    sys.path.insert(0, _dir)
    return _dir


def home_dir():
    server_role = os.environ.get(cfg4py.envar)

    if server_role == "DEV":
        os.makedirs("/tmp/backtest", exist_ok=True)
        return "/tmp/backtest"

    return path.expanduser("~/zillionare/backtest/")
