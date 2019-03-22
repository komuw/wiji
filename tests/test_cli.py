import sys
import logging
import argparse
from unittest import TestCase, mock

import cli

logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.CRITICAL)


class MockArgumentParser:
    def __init__(self, wiji_config, dry_run=True):
        self.wiji_config = wiji_config
        self.dry_run = dry_run

    def add_argument(self, *args, **kwargs):
        pass

    def parse_args(self, args=None, namespace=None):
        return argparse.Namespace(config=self.wiji_config, dry_run=self.dry_run, loglevel="DEBUG")


class TestCli(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_cli.TestCli.test_something
    """

    def setUp(self):
        self.parser = cli.cli.make_parser()
        self.wiji_config = "tests.testdata.cli.my_config.MyConfigInstance"

    def tearDown(self):
        pass

    def test_bad_args(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["wiji-cli", "-someBad", "-arguments"])

    def test_cli_success(self):
        with mock.patch("argparse.ArgumentParser") as mock_ArgumentParser:
            mock_ArgumentParser.return_value = MockArgumentParser(wiji_config=self.wiji_config)
            cli.cli.main()
