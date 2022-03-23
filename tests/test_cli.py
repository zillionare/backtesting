import subprocess
import unittest
from unittest import mock

from backtest import cli
from tests import find_free_port


class CliTest(unittest.TestCase):
    def test_cli(self):
        port = find_free_port()
        cli.status()
        cli.start(port)
        cli.stop()
