#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import os
import sys
from importlib.metadata import version
from os import path

import cfg4py
from omicron.core.backtestlog import BacktestLogger

logger = BacktestLogger.getLogger(__name__)


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


def endpoint():
    cfg = cfg4py.get_instance()

    major, minor, *_ = version("zillionare-backtest").split(".")
    prefix = cfg.server.prefix.rstrip("/")
    return f"{prefix}/v{major}.{minor}"
