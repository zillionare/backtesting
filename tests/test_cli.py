import os
import pickle
import unittest

from backtest import cli
from backtest.config import home_dir
from tests import find_free_port


class CliTest(unittest.TestCase):
    def test_cli(self):
        os.environ["https_proxy"] = ""
        os.environ["http_proxy"] = ""
        os.environ["all_proxy"] = ""

        port = find_free_port()
        cli.status()

        cli.start(port)

        state_file = os.path.join(home_dir(), "state.pkl")
        if os.path.exists(state_file):
            os.remove(state_file)

        cli.stop()

        self.assertTrue(os.path.exists(state_file))
