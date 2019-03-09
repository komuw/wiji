import sys
import asyncio
import logging
from unittest import TestCase, mock

import wiji


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


class TestHook(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_hook.TestHook.test_something
    """

    def setUp(self):
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
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_no_rlimit(self):
        with mock.patch("wiji.hook.SimpleHook.notify", new=AsyncMock()) as mock_hook_notify:
            mock_hook_notify.mock.return_value = None
            worker = wiji.Worker(the_task=self.myAdderTask, worker_id="myWorkerID1")

            # queue task
            kwargs = {"a": 78, "b": 101}
            self.myAdderTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])

            # consume
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["kwargs"], kwargs)
            self.assertTrue(mock_hook_notify.mock.called)
            self.assertEqual(
                mock_hook_notify.mock.call_args[1]["return_value"], kwargs["a"] + kwargs["b"]
            )
            self.assertEqual(mock_hook_notify.mock.call_args[1]["execution_exception"], None)
