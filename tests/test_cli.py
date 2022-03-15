import unittest

from backtest import cli


class CliTest(unittest.TestCase):
    def test_cli(self):
        cli.status()
        cli.start()
        cli.stop()
