import uuid
import signal
import asyncio
import argparse
from unittest import TestCase, mock

import cli
import wiji


def AsyncMock(*args, **kwargs):
    """
    see: https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
    """
    m = mock.MagicMock(*args, **kwargs)

    async def mock_coro(*args, **kwargs):
        return m(*args, **kwargs)

    mock_coro.mock = m
    return mock_coro


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
        self.wiji_config = "tests.testdata.cli.my_app.MyAppInstance"

    def tearDown(self):
        pass

    def test_bad_args(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["wiji-cli", "-someBad", "-arguments"])

    def test_cli_success(self):
        with mock.patch("argparse.ArgumentParser") as mock_ArgumentParser:
            mock_ArgumentParser.return_value = MockArgumentParser(wiji_config=self.wiji_config)
            cli.cli.main()


class TestCliSigHandling(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_cli.TestCliSigHandling.test_something
    """

    def setUp(self):
        class AdderTask(wiji.task.Task):
            the_broker = wiji.broker.InMemoryBroker()
            queue_name = "{0}-AdderTaskQueue".format(uuid.uuid4())
            drain_duration = 0.01

            async def run(self, a, b):
                return a + b

        _worker = wiji.Worker(the_task=AdderTask(), worker_id="myWorkerID1")
        self.workers = [_worker]
        self.logger = wiji.logger.SimpleLogger("wiji.TestCliSigHandling")

    def tearDown(self):
        pass

    @staticmethod
    def _run(coro):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_success_signal_handling(self):
        self._run(cli.utils.sig._signal_handling(logger=self.logger, workers=self.workers))

    def test_success_handle_termination_signal(self):
        self._run(
            cli.utils.sig._handle_termination_signal(
                logger=self.logger, _signal=signal.SIGTERM, workers=self.workers
            )
        )
