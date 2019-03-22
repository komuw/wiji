# do not to pollute the global namespace.
# see: https://python-packaging.readthedocs.io/en/latest/testing.html

import sys
import asyncio
import logging
from unittest import TestCase, mock

import wiji


logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.CRITICAL)


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
        self.BROKER = wiji.broker.InMemoryBroker()

        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        self.myAdderTask = AdderTask(the_broker=self.BROKER, queue_name=self.__class__.__name__)

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
            wiji.Worker(the_task="bad-task-arg")

        self.assertRaises(ValueError, mock_create_worker)
        with self.assertRaises(ValueError) as raised_exception:
            mock_create_worker()
        self.assertIn(
            "`the_task` should be of type:: `wiji.task.Task`", str(raised_exception.exception)
        )

    def test_bad_args(self):
        def mock_create_worker():
            wiji.Worker(the_task=self.myAdderTask, worker_id=92033)

        self.assertRaises(ValueError, mock_create_worker)
        with self.assertRaises(ValueError) as raised_exception:
            mock_create_worker()
        self.assertIn(
            "`worker_id` should be of type:: `None` or `str`", str(raised_exception.exception)
        )

    def test_success_instantiation(self):
        wiji.Worker(the_task=self.myAdderTask, worker_id="myWorkerID1")

    def test_retries(self):
        res = wiji.Worker._retry_after(-110)
        self.assertEqual(res, 30)

        res = wiji.Worker._retry_after(5)
        self.assertTrue(res > 60 * (2 ** 5))

        for i in [6, 7, 34]:
            res = wiji.Worker._retry_after(i)
            self.assertTrue(res > 16 * 60)

    def test_consume_tasks(self):
        worker = wiji.Worker(the_task=self.myAdderTask, worker_id="myWorkerID1")

        # queue task
        kwargs = {"a": 78, "b": 101}
        self.myAdderTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])

        # consume
        dequeued_item = self._run(worker.consume_tasks(TESTING=True))
        self.assertEqual(dequeued_item["version"], 1)
        self.assertEqual(dequeued_item["current_retries"], 0)
        self.assertEqual(dequeued_item["max_retries"], 0)
        self.assertEqual(dequeued_item["args"], [])
        self.assertEqual(dequeued_item["kwargs"], kwargs)

        # queue task
        self.myAdderTask.synchronous_delay(34, 88)
        # consume
        dequeued_item = self._run(worker.consume_tasks(TESTING=True))
        self.assertEqual(dequeued_item["version"], 1)
        self.assertEqual(dequeued_item["current_retries"], 0)
        self.assertEqual(dequeued_item["max_retries"], 0)
        self.assertEqual(dequeued_item["args"], [34, 88])
        self.assertEqual(dequeued_item["kwargs"], {})
