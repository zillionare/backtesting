import os
import unittest

import cfg4py

from backtest import cli
from backtest.config import get_config_dir, home_dir
from tests import find_free_port


class CliTest(unittest.TestCase):
    def test_cli(self):
        os.environ["https_proxy"] = ""
        os.environ["http_proxy"] = ""
        os.environ["all_proxy"] = ""

        cfg4py.init(get_config_dir())
        port = find_free_port()
        cli.status()

        cli.start(port)

        state_file = os.path.join(home_dir(), "backtest.index.json")
        if os.path.exists(state_file):
            os.remove(state_file)

        cli.stop()

        self.assertTrue(os.path.exists(state_file))
