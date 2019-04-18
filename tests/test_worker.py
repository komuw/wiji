# do not to pollute the global namespace.
# see: https://python-packaging.readthedocs.io/en/latest/testing.html

import os
import uuid
import json
import asyncio
from unittest import TestCase, mock

import wiji
import docker

from .utils import ExampleRedisBroker


def AsyncMock(*args, **kwargs):
    """
    see: https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
    """
    m = mock.MagicMock(*args, **kwargs)

    async def mock_coro(*args, **kwargs):
        return m(*args, **kwargs)

    mock_coro.mock = m
    return mock_coro


class ExampleAdderInMemTask(wiji.task.Task):
    the_broker = wiji.broker.InMemoryBroker()
    queue_name = "{0}-ExampleAdderInMemTaskQueue".format(uuid.uuid4())

    async def run(self, a, b):
        res = a + b
        return res


class ExampleAdderRedisTask(wiji.task.Task):
    the_broker = ExampleRedisBroker()
    queue_name = "{0}-ExampleAdderRedisTaskQueue".format(uuid.uuid4())

    async def run(self, a, b):
        res = a + b
        return res


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
        self.myTask = ExampleAdderInMemTask()
        # `queue_name` should be unique. one task, one queue.
        # However for tests, we want to re-use the same task in multiple tests instead of creating
        # new tasks per test. So we have to change the `queue_name` per test
        self.myTask.queue_name = "{0}-ExampleAdderInMemTaskQueue".format(uuid.uuid4())

    def tearDown(self):
        pass

    def broker_path(self):
        return self.BROKER.__module__ + "." + self.BROKER.__class__.__name__

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
        self.assertEqual(dequeued_item["task_options"]["current_retries"], 0)
        self.assertEqual(dequeued_item["task_options"]["max_retries"], 0)
        self.assertEqual(dequeued_item["task_options"]["args"], [])
        self.assertEqual(dequeued_item["task_options"]["kwargs"], kwargs)

        # queue task
        self.myTask.synchronous_delay(34, 88)
        # consume
        dequeued_item = self._run(worker.consume_tasks(TESTING=True))
        self.assertEqual(dequeued_item["version"], 1)
        self.assertEqual(dequeued_item["task_options"]["current_retries"], 0)
        self.assertEqual(dequeued_item["task_options"]["max_retries"], 0)
        self.assertEqual(dequeued_item["task_options"]["args"], [34, 88])
        self.assertEqual(dequeued_item["task_options"]["kwargs"], {})

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
            "task_options": {
                "task_id": "f5ceee05-5e41-4fc4-8e2e-d16aa6d67bff",
                "eta": "2019-03-24T16:00:12.247687+00:00",
                "current_retries": 0,
                "max_retries": 0,
                "hook_metadata": "",
                "args": [],
                "kwargs": {"a": 21, "b": 535},
            },
        }

        with mock.patch(
            "{broker_path}.dequeue".format(broker_path=self.broker_path()), new=AsyncMock()
        ) as mock_dequeue:
            mock_dequeue.mock.return_value = json.dumps(item)
            worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
            # queue and consume task
            self.myTask.synchronous_delay(a=21, b=535)
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            self.assertTrue(mock_dequeue.mock.called)
            self.assertEqual(mock_dequeue.mock.call_args[1], {"queue_name": self.myTask.queue_name})

    def test_eta_respected(self):
        kwargs = {"a": 7121, "b": 6122}

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
        kwargs = {"a": 263_342, "b": 832_429}
        with mock.patch("wiji.worker.Worker.run_task", new=AsyncMock()) as mock_run_task:
            worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
            self.myTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            self.assertTrue(mock_run_task.mock.called)
            self.assertEqual(mock_run_task.mock.call_args[1]["a"], kwargs["a"])
            self.assertEqual(mock_run_task.mock.call_args[1]["b"], kwargs["b"])

    def test_broker_done_called(self):
        kwargs = {"a": 263_342, "b": 832_429}
        with mock.patch(
            "{broker_path}.done".format(broker_path=self.broker_path()), new=AsyncMock()
        ) as mock_broker_done:
            worker = wiji.Worker(the_task=self.myTask, worker_id="myWorkerID1")
            self.myTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            self.assertTrue(mock_broker_done.mock.called)
            self.assertEqual(
                json.loads(mock_broker_done.mock.call_args[1]["item"])["task_options"]["kwargs"],
                kwargs,
            )
            self.assertEqual(
                mock_broker_done.mock.call_args[1]["queue_name"], self.myTask.queue_name
            )
            self.assertEqual(
                mock_broker_done.mock.call_args[1]["state"], wiji.task.TaskState.EXECUTED
            )

            task_options = json.loads(mock_broker_done.mock.call_args[1]["item"])["task_options"]
            self.assertEqual(task_options["kwargs"], kwargs)
            self.assertIsNotNone(task_options["task_id"])
            self.assertEqual(len(task_options["task_id"]), 36)  # len of uuid4

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
            the_broker = self.BROKER
            queue_name = "{0}-DividerTaskQueue".format(uuid.uuid4())

            async def run(self, a):
                res = a / 3
                print("divider res: ", res)
                return res

        class AdderTask(wiji.task.Task):
            the_broker = self.BROKER
            queue_name = "{0}-AdderTaskQueue".format(uuid.uuid4())
            chain = DividerTask

            async def run(self, a, b):
                res = a + b
                return res

        MYAdderTask = AdderTask()

        kwargs = {"a": 400, "b": 603}
        worker = wiji.Worker(the_task=MYAdderTask, worker_id="myWorkerID1")
        MYAdderTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])

        with mock.patch.object(
            AdderTask, "delay", new=AsyncMock()
        ) as mock_adder_delay, mock.patch.object(
            DividerTask, "delay", new=AsyncMock()
        ) as mock_divider_delay:
            mock_adder_delay.mock.return_value = None
            mock_divider_delay.return_value = None

            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            # adder task is not queued
            self.assertFalse(mock_adder_delay.mock.called)
            # but divider task(the chain) is queued
            self.assertTrue(mock_divider_delay.mock.called)
            self.assertEqual(mock_divider_delay.mock.call_args[0][1], kwargs["a"] + kwargs["b"])

    def test_no_chaining_if_exception(self):
        """
        test that if parent task raises exception, the chained task is not queued
        """

        class DividerTask(wiji.task.Task):
            the_broker = self.BROKER
            queue_name = "{0}-DividerTaskQueue".format(uuid.uuid4())

            async def run(self, a):
                res = a / 3
                print("divider res: ", res)
                return res

        class AdderTask(wiji.task.Task):
            the_broker = self.BROKER
            queue_name = "{0}-AdderTaskQueue".format(uuid.uuid4())

            async def run(self, a, b):
                return await self.do_work(a, b)

            @staticmethod
            async def do_work(a, b):
                return a + b

        MYAdderTask = AdderTask()

        kwargs = {"a": 400, "b": 603}
        worker = wiji.Worker(the_task=MYAdderTask, worker_id="myWorkerID1")
        MYAdderTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])

        with mock.patch(
            "wiji.task.Task.delay", new=AsyncMock()
        ) as mock_task_delay, mock.patch.object(
            AdderTask, "do_work", side_effect=Exception("test_no_chaining_if_exception")
        ) as mock_do_work:
            mock_task_delay.mock.return_value = None
            _ = mock_do_work

            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)
            # chain is not queued
            self.assertFalse(mock_task_delay.mock.called)

    def test_no_chaining_if_retrying(self):
        """
        test that if parent task is been retried, the chained task is not queued
        """

        class DividerTask(wiji.task.Task):
            the_broker = self.BROKER
            queue_name = "{0}-DividerTaskQueue".format(uuid.uuid4())

            async def run(self, a):
                res = a / 3
                print("divider res: ", res)
                return res

        class AdderTask(wiji.task.Task):
            the_broker = self.BROKER
            queue_name = "{0}-AdderTaskQueue".format(uuid.uuid4())
            chain = DividerTask

            async def run(self, a, b):
                res = a + b
                await self.retry(a=221, b=555, task_options=wiji.task.TaskOptions(max_retries=2))
                return res

        MYAdderTask = AdderTask()

        kwargs = {"a": 400, "b": 603}
        worker = wiji.Worker(the_task=MYAdderTask, worker_id="myWorkerID1")
        MYAdderTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])

        with mock.patch.object(
            AdderTask, "delay", new=AsyncMock()
        ) as mock_adder_delay, mock.patch.object(
            DividerTask, "delay", new=AsyncMock()
        ) as mock_divider_delay:
            mock_adder_delay.mock.return_value = None
            mock_divider_delay.return_value = None

            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            # divider chain is not queued
            self.assertFalse(mock_divider_delay.mock.called)
            # but adder task is queued again
            self.assertTrue(mock_adder_delay.mock.called)

    def test_shutdown(self):
        class AdderTask(wiji.task.Task):
            the_broker = self.BROKER
            # we want this task to be processed slowly
            the_ratelimiter = wiji.ratelimiter.SimpleRateLimiter(execution_rate=1.0)
            queue_name = "{0}-TestWorker.test_shutdown".format(uuid.uuid4())
            drain_duration = 1.0

            async def run(self, a, b):
                res = a + b
                return res

        _myTask = AdderTask()
        worker = wiji.Worker(the_task=_myTask, worker_id="myWorkerID1")
        self.assertFalse(worker.SUCCESFULLY_SHUT_DOWN)

        # queue a lot of tasks
        queued = []
        for i in range(1, 20):
            _myTask.synchronous_delay(a=9001, b=i)
            queued.append(i)

        async def call_worker_shutdown():
            """
            sleep for a few seconds so that some tasks can be consumed,
            then shutdown worker
            """
            await asyncio.sleep(5)
            await worker.shutdown()

        loop = asyncio.get_event_loop()
        tasks = asyncio.gather(
            worker.consume_tasks(TESTING=False), call_worker_shutdown(), loop=loop
        )
        loop.run_until_complete(tasks)

        # assert that some tasks have been consumed and also
        # that not all were consumed.
        self.assertTrue(_myTask.the_broker._llen(AdderTask.queue_name) > 10)
        self.assertTrue(_myTask.the_broker._llen(AdderTask.queue_name) < len(queued))
        self.assertTrue(worker.SUCCESFULLY_SHUT_DOWN)

    def test_broker_shutdown_called(self):
        with mock.patch(
            "{broker_path}.shutdown".format(broker_path=self.broker_path()), new=AsyncMock()
        ) as mock_broker_shutdown:

            class AdderTask(wiji.task.Task):
                the_broker = self.BROKER
                # we want this task to be processed slowly
                the_ratelimiter = wiji.ratelimiter.SimpleRateLimiter(execution_rate=1.0)
                queue_name = "{0}-TestWorker.test_shutdown".format(uuid.uuid4())
                drain_duration = 1.0

                async def run(self, a, b):
                    res = a + b
                    return res

            _myTask = AdderTask()
            worker = wiji.Worker(the_task=_myTask, worker_id="myWorkerID1")
            self.assertFalse(worker.SUCCESFULLY_SHUT_DOWN)

            # queue a lot of tasks
            queued = []
            for i in range(1, 20):
                _myTask.synchronous_delay(a=9001, b=i)
                queued.append(i)

            async def call_worker_shutdown():
                """
                sleep for a few seconds so that some tasks can be consumed,
                then shutdown worker
                """
                await asyncio.sleep(5)
                await worker.shutdown()

            loop = asyncio.get_event_loop()
            tasks = asyncio.gather(
                worker.consume_tasks(TESTING=False), call_worker_shutdown(), loop=loop
            )
            loop.run_until_complete(tasks)

            self.assertTrue(mock_broker_shutdown.mock.called)
            self.assertEqual(
                mock_broker_shutdown.mock.call_args[1]["queue_name"], AdderTask.queue_name
            )


class TestWorkerRedisBroker(TestWorker):
    """
    re-run the worker tests but this time round use a redis broker.

    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_worker.TestWorkerRedisBroker
    """

    def setUp(self):
        super().setUp()
        self.BROKER = ExampleRedisBroker()
        self.myTask = ExampleAdderRedisTask()
        self.myTask.queue_name = "{0}-ExampleAdderRedisTaskQueue".format(uuid.uuid4())

        self._setup_docker()
        # ensure each testcase starts off with a fresh/clean broker
        self.BROKER._flushdb()

    @staticmethod
    def _setup_docker():
        if os.environ.get("IN_DOCKER"):
            # will utilise an already existing redis docker container
            return

        docker_client = docker.from_env()
        running_containers = docker_client.containers.list()
        for container in running_containers:
            container.stop()

        name = os.environ.get("WIJI_TEST_REDIS_CONTAINER_NAME", "wiji_test_redis_container")
        docker_client.containers.run(
            "redis:3.0-alpine",
            name=name,
            detach=True,
            auto_remove=True,
            labels={"name": name, "use": "running_wiji_tets"},
            ports={"6379/tcp": 6379},
            publish_all_ports=True,
            stdout=True,
            stderr=True,
        )
