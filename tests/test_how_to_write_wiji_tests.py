import asyncio
from unittest import TestCase, mock

import wiji

from .utils import ExampleRedisBroker


DATABASE = {}


class AdderTask(wiji.task.Task):
    the_broker = ExampleRedisBroker()
    queue_name = "AdderTask"

    async def run(self, a, b):
        """
        adds two numbers and stores the resut in a database
        """
        result = a + b
        DATABASE["result"] = result
        return result


class ExampleView:
    def post(self, request: dict):
        a = request["a"]
        b = request["b"]
        AdderTask().synchronous_delay(a=a, b=b)


class TestExampleView(TestCase):
    """
    TestCase to showcase how users of `wiji` can write tests for their code that is using wiji
   

    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_how_to_write_wiji_tests.TestExampleView.test_something
    """

    @staticmethod
    def _run(coro):
        """
        helper function that runs any coroutine in an event loop and passes its return value back to the caller.
        https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
        """
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_writing_wiji_tests(self):
        with mock.patch.object(
            AdderTask, "the_broker", wiji.broker.InMemoryBroker()
        ) as mock_broker:
            self.assertDictEqual(DATABASE, {})
            view = ExampleView()
            view.post(request={"a": 45, "b": 46})

            self.assertIsInstance(AdderTask.the_broker, wiji.broker.InMemoryBroker)
            self.assertNotIsInstance(AdderTask.the_broker, ExampleRedisBroker)
            self.assertEqual(mock_broker._llen(AdderTask.queue_name), 1)

            worker = wiji.Worker(the_task=AdderTask())
            self._run(worker.consume_tasks(TESTING=True))
            self.assertDictEqual(DATABASE, {"result": 91})
