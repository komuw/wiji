# do not to pollute the global namespace.
# see: https://python-packaging.readthedocs.io/en/latest/testing.html

import sys
import json
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

        self.myTask = AdderTask(the_broker=self.BROKER, queue_name=self.__class__.__name__)

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
            wiji.Worker(the_task=self.myTask, worker_id=92033)

        self.assertRaises(ValueError, mock_create_worker)
        with self.assertRaises(ValueError) as raised_exception:
            mock_create_worker()
        self.assertIn(
            "`worker_id` should be of type:: `None` or `str`", str(raised_exception.exception)
        )

    def test_success_instantiation(self):
        wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")

    def test_retries(self):
        res = wiji.Worker._retry_after(-110)
        self.assertEqual(res, 30)

        res = wiji.Worker._retry_after(5)
        self.assertTrue(res > 60 * (2 ** 5))

        for i in [6, 7, 34]:
            res = wiji.Worker._retry_after(i)
            self.assertTrue(res > 16 * 60)

    def test_consume_tasks(self):
        worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")

        # queue task
        kwargs = {"a": 78, "b": 101}
        self.myTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])

        # consume
        dequeued_item = self._run(worker.consume_tasks(TESTING=True))
        self.assertEqual(dequeued_item["version"], 1)
        self.assertEqual(dequeued_item["current_retries"], 0)
        self.assertEqual(dequeued_item["max_retries"], 0)
        self.assertEqual(dequeued_item["args"], [])
        self.assertEqual(dequeued_item["kwargs"], kwargs)

        # queue task
        self.myTask.synchronous_delay(34, 88)
        # consume
        dequeued_item = self._run(worker.consume_tasks(TESTING=True))
        self.assertEqual(dequeued_item["version"], 1)
        self.assertEqual(dequeued_item["current_retries"], 0)
        self.assertEqual(dequeued_item["max_retries"], 0)
        self.assertEqual(dequeued_item["args"], [34, 88])
        self.assertEqual(dequeued_item["kwargs"], {})

    def test_broker_check_called(self):
        with mock.patch("wiji.task.Task._broker_check", new=AsyncMock()) as mock_broker_check:
            worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
            # queue and consume task
            self.myTask.synchronous_delay(a=21, b=535)
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            self.assertTrue(mock_broker_check.mock.called)
            self.assertEqual(mock_broker_check.mock.call_args[1], {"from_worker": True})

    def test_ratelimit_called(self):
        with mock.patch(
            "wiji.ratelimiter.SimpleRateLimiter.limit", new=AsyncMock()
        ) as mock_ratelimit:
            worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
            # queue and consume task
            self.myTask.synchronous_delay(a=21, b=535)
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            self.assertTrue(mock_ratelimit.mock.called)

    def test_broker_dequeue_called(self):
        item = {
            "version": 1,
            "task_id": "f5ceee05-5e41-4fc4-8e2e-d16aa6d67bff",
            "eta": "2019-03-24T16:00:12.247687+00:00",
            "current_retries": 0,
            "max_retries": 0,
            "log_id": "",
            "hook_metadata": "",
            "args": [],
            "kwargs": {"a": 21, "b": 535},
        }

        with mock.patch("wiji.broker.InMemoryBroker.dequeue", new=AsyncMock()) as mock_dequeue:
            mock_dequeue.mock.return_value = json.dumps(item)
            worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
            # queue and consume task
            self.myTask.synchronous_delay(a=21, b=535)
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            self.assertTrue(mock_dequeue.mock.called)
            self.assertEqual(mock_dequeue.mock.call_args[1], {"queue_name": self.myTask.queue_name})

    def test_eta_respected(self):
        kwargs = {"a": 21, "b": 535}

        # NO re-queuing is carried out
        worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
        self.myTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])
        with mock.patch("wiji.task.Task.delay", new=AsyncMock()) as mock_task_delay:
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)
            self.assertFalse(mock_task_delay.mock.called)

        # re-queuing is carried out
        worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
        self.myTask.synchronous_delay(
            a=kwargs["a"], b=kwargs["b"], tas_options=wiji.task.TaskOptions(eta=788.99)
        )
        with mock.patch("wiji.task.Task.delay", new=AsyncMock()) as mock_task_delay:
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)
            self.assertTrue(mock_task_delay.mock.called)
            self.assertEqual(mock_task_delay.mock.call_args[1], kwargs)

    def test_run_task_called(self):
        kwargs = {"a": 263342, "b": 832429}
        with mock.patch("wiji.worker.Worker.run_task", new=AsyncMock()) as mock_run_task:
            worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
            self.myTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            self.assertTrue(mock_run_task.mock.called)
            self.assertEqual(mock_run_task.mock.call_args[1], kwargs)

    def test_broker_done_called(self):
        kwargs = {"a": 263342, "b": 832429}
        with mock.patch("wiji.broker.InMemoryBroker.done", new=AsyncMock()) as mock_broker_done:
            worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
            self.myTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            self.assertTrue(mock_broker_done.mock.called)
            self.assertEqual(
                json.loads(mock_broker_done.mock.call_args[1]["item"])["kwargs"], kwargs
            )
            self.assertEqual(
                mock_broker_done.mock.call_args[1]["queue_name"], self.myTask.queue_name
            )
            self.assertEqual(
                mock_broker_done.mock.call_args[1]["state"], wiji.task.TaskState.EXECUTED
            )

            task_options = mock_broker_done.mock.call_args[1]["task_options"]
            self.assertEqual(task_options.kwargs, kwargs)
            self.assertIsNotNone(task_options.task_id)
            self.assertEqual(len(task_options.task_id), 36)  # len of uuid4

    def test_task_no_chain(self):
        """
        test task with NO chain does not call task.delay
        """
        kwargs = {"a": 400, "b": 901}

        worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
        self.myTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])
        with mock.patch("wiji.task.Task.delay", new=AsyncMock()) as mock_task_delay:
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)
            self.assertFalse(mock_task_delay.mock.called)

    def test_task_with_chain(self):
        """
        test task with chain CALLS task.delay
        """

        class DividerTask(wiji.task.Task):
            async def run(self, a):
                res = a / 3
                print("divider res: ", res)
                return res

        MYDividerTask = DividerTask(the_broker=self.BROKER, queue_name="DividerTaskChainQueue")

        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        MYAdderTask = AdderTask(
            the_broker=self.BROKER, queue_name="AdderTaskChainQueue", chain=MYDividerTask
        )

        kwargs = {"a": 400, "b": 603}
        worker = wiji.Worker(the_task=MYAdderTask, worker_id="myWorkerID1")
        MYAdderTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])

        with mock.patch("wiji.task.Task.delay", new=AsyncMock()) as mock_task_delay:
            mock_task_delay.mock.return_value = None
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)
            self.assertTrue(mock_task_delay.mock.called)
            self.assertEqual(mock_task_delay.mock.call_args[0][1], kwargs["a"] + kwargs["b"])
