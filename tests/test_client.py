# do not to pollute the global namespace.
# see: https://python-packaging.readthedocs.io/en/latest/testing.html

import os
import sys
import json
import asyncio
import logging
from unittest import TestCase

import xyzabc
import mock


logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.DEBUG)


def AsyncMock(*args, **kwargs):
    """
    see: https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
    """
    m = mock.MagicMock(*args, **kwargs)

    async def mock_coro(*args, **kwargs):
        return m(*args, **kwargs)

    mock_coro.mock = m
    return mock_coro


class TestWorker(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_worker.TestWorker.test_can_connect
    """

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.cli = xyzabc.Worker()

    def tearDown(self):
        pass

    @staticmethod
    def _run(coro):
        """
        helper function that runs any coroutine in an event loop and passes its return value back to the caller.
        https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
        """
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_bad_instantiation(self):
        def mock_create_worker():
            xyzabc.Worker()

        self.assertRaises(ValueError, mock_create_worker)
        with self.assertRaises(ValueError) as raised_exception:
            mock_create_worker()
        self.assertIn("log_metadata should be of type", str(raised_exception.exception))

