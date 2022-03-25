import os
import unittest

from backtest import cli
from tests import find_free_port


class CliTest(unittest.TestCase):
    def test_cli(self):
        os.environ["https_proxy"] = ""
        os.environ["http_proxy"] = ""
        os.environ["all_proxy"] = ""

        port = find_free_port()
        cli.status()
        cli.start(port)
        cli.stop()
