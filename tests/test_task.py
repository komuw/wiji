# do not to pollute the global namespace.
# see: https://python-packaging.readthedocs.io/en/latest/testing.html

import os
import sys
import json
import asyncio
import inspect
import logging
from unittest import TestCase

import wiji
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


class TestTypeChecking(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_task.TestTypeChecking.test_something
    """

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.BROKER = wiji.broker.SimpleBroker()

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

    def test_type_check_error(self):
        def foo(a, b, *args, c, d=10, **kwargs):
            pass

        def carry_out_checking():
            return wiji.task.Task._type_check(foo, 1, 4)

        self.assertRaises(TypeError, carry_out_checking)
        with self.assertRaises(TypeError) as raised_exception:
            carry_out_checking()
        self.assertIn("missing a required argument: 'c'", str(raised_exception.exception))

    def test_type_check_success(self):
        def foo(a, b, *args, c, d=10, **kwargs):
            pass

        def carry_out_checking():
            return wiji.task.Task._type_check(foo, 1, 4, c="PP")

        x = carry_out_checking()
        self.assertTrue(x.signature == inspect.signature(foo))

    def test_type_check_task_error(self):
        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        myAdderTask = AdderTask(the_broker=self.BROKER, queue_name="testTaskQ1")

        def call_delay():
            myAdderTask.synchronous_delay(4, 6, 9, task_options=wiji.task.TaskOptions(eta=4.56))

        self.assertRaises(TypeError, call_delay)
        with self.assertRaises(TypeError) as raised_exception:
            call_delay()
        self.assertIn("too many positional arguments", str(raised_exception.exception))

    def test_type_check_task_success(self):
        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        myAdderTask = AdderTask(the_broker=self.BROKER, queue_name="testTaskQ1")

        def call_delay():
            myAdderTask.synchronous_delay(4, 6, task_options=wiji.task.TaskOptions(eta=4.56))

        call_delay()
