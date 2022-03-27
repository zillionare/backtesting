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

        state_file = os.path.join(home_dir(), "state.pkl")
        if os.path.exists(state_file):
            os.remove(state_file)

        port = find_free_port()
        cli.status()
        cli.start(port)
        cli.stop()

        # check pickle
        with open(state_file, "rb") as f:
            brokers = pickle.load(f)
            self.assertDictEqual({}, brokers)
