import os
import json
import time
import random
import string
import typing
import logging
import asyncio
import datetime

from . import task
from . import protocol
from . import watchdog
from . import ratelimiter


class Worker:
    """
    The only time this worker coroutine should ever raise an Exception is either:
      - during class instantiation
      - when the worker is about to start consuming tasks
    """

    def __init__(
        self,
        the_task: task.Task,
        worker_id: typing.Union[None, str] = None,
        use_watchdog: bool = False,
        watchdog_duration: float = 0.1,
    ) -> None:
        """
        """
        self._validate_worker_args(
            the_task=the_task,
            worker_id=worker_id,
            use_watchdog=use_watchdog,
            watchdog_duration=watchdog_duration,
        )

        self._PID = os.getpid()
        self.the_task = the_task
        if worker_id is not None:
            self.worker_id = worker_id
        else:
            self.worker_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))

        if typing.TYPE_CHECKING:
            assert isinstance(self.the_task.log_metadata, dict)
        self.the_task.log_metadata.update({"worker_id": self.worker_id, "process_id": self._PID})
        self.the_task.logger.bind(
            level=self.the_task.loglevel, log_metadata=self.the_task.log_metadata
        )

        self.use_watchdog = use_watchdog
        self.watchdog_duration = watchdog_duration

        self.watchdog = None
        if self.use_watchdog:
            if typing.TYPE_CHECKING:
                assert isinstance(self.the_task.task_name, str)
            self.watchdog = watchdog.BlockingWatchdog(
                watchdog_duration=self.watchdog_duration, task_name=self.the_task.task_name
            )

        self.SHOULD_SHUT_DOWN: bool = False
        self.SUCCESFULLY_SHUT_DOWN: bool = False

        self.the_task._sanity_check_logger(event="worker_sanity_check_logger")

    def _validate_worker_args(
        self,
        the_task: task.Task,
        worker_id: typing.Union[None, str],
        use_watchdog: bool,
        watchdog_duration: float,
    ) -> None:
        if not isinstance(the_task, task.Task):
            raise ValueError(
                "`the_task` should be of type:: `wiji.task.Task` You entered: {0}".format(
                    type(the_task)  # for this `the_task._debug_task_name` may be unavailable
                )
            )
        if not isinstance(worker_id, (type(None), str)):
            raise ValueError(
                "Task: {0}. `worker_id` should be of type:: `None` or `str` You entered: {1}".format(
                    the_task._debug_task_name, type(worker_id)
                )
            )
        if not isinstance(use_watchdog, bool):
            raise ValueError(
                "Task: {0}. `use_watchdog` should be of type:: `bool` You entered: {1}".format(
                    the_task._debug_task_name, type(use_watchdog)
                )
            )
        if not isinstance(watchdog_duration, float):
            raise ValueError(
                "Task: {0}. `watchdog_duration` should be of type:: `float` You entered: {1}".format(
                    the_task._debug_task_name, type(watchdog_duration)
                )
            )

    def _log(self, level: typing.Union[str, int], log_data: dict) -> None:
        try:
            self.the_task.logger.log(level, log_data)
        except Exception:
            pass

    @staticmethod
    def _retry_after(current_retries: int) -> int:
        """
        returns the number of seconds to retry after.
        retries will happen in this sequence;
        0.5min, 1min, 2min, 4min, 8min, 16min, 32min, 16min, 16min, 16min ...
        """
        # TODO:
        # 1. give users ability to bring their own retry algorithms.
        if current_retries < 0:
            current_retries = 0

        jitter = random.randint(60, 180)  # 1min-3min
        if current_retries in [0, 1]:
            return int(0.5 * 60)  # 0.5min
        elif current_retries == 2:
            return 1 * 60
        elif current_retries >= 6:
            return (16 * 60) + jitter  # 16 minutes + jitter
        else:
            return (60 * (2 ** current_retries)) + jitter

    async def _notify_broker(self, item: str, queue_name: str, state: task.TaskState) -> None:
        try:
            await self.the_task.the_broker.done(queue_name=queue_name, item=item, state=state)
        except Exception as e:
            self._log(
                logging.ERROR,
                {
                    "event": "wiji.Worker.consume_tasks",
                    "stage": "end",
                    "state": "broker done error",
                    "error": str(e),
                },
            )

    async def run_task(self, *task_args: typing.Any, **task_kwargs: typing.Any) -> None:
        task_options = task_kwargs.pop("task_options", {})
        await self.the_task._notify_hook(
            task_id=task_options.get("task_id"),
            state=task.TaskState.EXECUTING,
            hook_metadata=task_options.get("hook_metadata"),
        )
        if self.watchdog is not None:
            self.watchdog.notify_alive_before()

        return_value = None
        execution_exception = None
        thread_time_start = time.thread_time()
        perf_counter_start = time.perf_counter()
        monotonic_start = time.monotonic()
        process_time_start = time.process_time()
        try:
            return_value = await self.the_task.run(*task_args, **task_kwargs)
            if self.the_task.the_chain and not self.the_task._RETRYING:
                # enqueue the chained task using the return_value
                await self.the_task.the_chain.delay(return_value)
            if self.the_task._RETRYING:
                # task is been retried
                self._log(
                    logging.INFO,
                    {
                        "event": "wiji.Worker.run_task",
                        "state": "Task is been retried.",
                        "stage": "end",
                        "task_name": self.the_task.task_name,
                        "current_retries": task_options.get("current_retries"),
                        "max_retries": task_options.get("max_retries"),
                    },
                )
        except Exception as e:
            execution_exception = e
            self._log(
                logging.ERROR,
                {
                    "event": "wiji.Worker.run_task",
                    "stage": "end",
                    "state": "task execution error",
                    "error": str(e),
                },
            )
        finally:
            thread_time_end = time.thread_time()
            perf_counter_end = time.perf_counter()
            monotonic_end = time.monotonic()
            process_time_end = time.process_time()
            execution_duration = {
                "thread_time": float("{0:.4f}".format(thread_time_end - thread_time_start)),
                "perf_counter": float("{0:.4f}".format(perf_counter_end - perf_counter_start)),
                "monotonic": float("{0:.4f}".format(monotonic_end - monotonic_start)),
                "process_time": float("{0:.4f}".format(process_time_end - process_time_start)),
            }
            await self.the_task._notify_ratelimiter(
                task_id=task_options.get("task_id"),
                state=task.TaskState.EXECUTED,
                execution_duration=execution_duration,
                execution_exception=execution_exception,
                return_value=return_value,
            )
            await self.the_task._notify_hook(
                task_id=task_options.get("task_id"),
                state=task.TaskState.EXECUTED,
                hook_metadata=task_options.get("hook_metadata"),
                execution_duration=execution_duration,
                execution_exception=execution_exception,
                return_value=return_value,
            )
            if self.watchdog is not None:
                self.watchdog.notify_alive_after()

    async def consume_tasks(
        self, TESTING: bool = False
    ) -> typing.Union[None, typing.Dict[str, typing.Any]]:
        """
        In loop; dequeues items from the :attr:`queue <Worker.queue>` and calls :func:`run <Worker.run>`.

        Parameters:
            TESTING: indicates whether this method is been called while running tests.
        """
        # this can exit with error
        await self.the_task._broker_check(from_worker=True)

        if self.watchdog is not None:
            self.watchdog.start()

        dequeue_retry_count = 0
        while True:
            self._log(logging.INFO, {"event": "wiji.Worker.consume_tasks", "stage": "start"})
            if self.SHOULD_SHUT_DOWN:
                self._log(
                    logging.INFO,
                    {
                        "event": "wiji.Worker.consume_tasks",
                        "stage": "end",
                        "state": "cleanly shutting down worker.",
                    },
                )
                return None

            try:
                if typing.TYPE_CHECKING:
                    # make mypy happy
                    # https://github.com/python/mypy/issues/4805
                    assert isinstance(self.the_task.the_ratelimiter, ratelimiter.BaseRateLimiter)

                # rate limit ourselves
                await self.the_task.the_ratelimiter.limit()
            except Exception as e:
                self._log(
                    logging.ERROR,
                    {
                        "event": "wiji.Worker.consume_tasks",
                        "stage": "end",
                        "state": "consume_tasks error",
                        "error": str(e),
                    },
                )
                continue

            try:
                _dequeued_item: str = await self.the_task.the_broker.dequeue(
                    queue_name=self.the_task.queue_name
                )
                dequeued_item: dict = json.loads(_dequeued_item)
            except Exception as e:
                poll_queue_interval = self._retry_after(dequeue_retry_count)
                dequeue_retry_count += 1
                self._log(
                    logging.ERROR,
                    {
                        "event": "wiji.Worker.consume_tasks",
                        "stage": "end",
                        "state": "dequeue tasks failed. sleeping for {0}minutes".format(
                            poll_queue_interval / 60
                        ),
                        "dequeue_retry_count": dequeue_retry_count,
                        "error": str(e),
                    },
                )
                await asyncio.sleep(poll_queue_interval)
                continue

            # dequeue succeded
            dequeue_retry_count = 0
            try:
                _ = dequeued_item["version"]
                _task_options = dequeued_item["task_options"]
                task_id = _task_options["task_id"]
                task_eta = _task_options["eta"]
                task_current_retries = _task_options["current_retries"]
                task_max_retries = _task_options["max_retries"]
                task_hook_metadata = _task_options["hook_metadata"]
                task_args = _task_options["args"]
                task_kwargs = _task_options["kwargs"]

                task_kwargs.update(
                    {
                        "task_options": {
                            "task_id": task_id,
                            "eta": task_eta,
                            "current_retries": task_current_retries,
                            "max_retries": task_max_retries,
                            "hook_metadata": task_hook_metadata,
                        }
                    }
                )
            except KeyError as e:
                e = KeyError("enqueued message/object is missing required field: {}".format(str(e)))
                self._log(
                    logging.ERROR,
                    {
                        "event": "wiji.Worker.consume_tasks",
                        "stage": "end",
                        "state": "consume_tasks error",
                        "error": str(e),
                    },
                )
                continue

            await self.the_task._notify_hook(
                task_id=task_id, state=task.TaskState.DEQUEUED, hook_metadata=task_hook_metadata
            )

            now = datetime.datetime.now(tz=datetime.timezone.utc)
            if protocol.Protocol._from_isoformat(task_eta) <= now:
                await self.run_task(*task_args, **task_kwargs)
                await self._notify_broker(
                    item=_dequeued_item,
                    queue_name=self.the_task.queue_name,
                    state=task.TaskState.EXECUTED,
                )
            else:
                # respect eta
                task_kwargs.pop("task_options", None)
                await self.the_task.delay(*task_args, **task_kwargs)
            self._log(
                logging.INFO,
                {"event": "wiji.Worker.consume_tasks", "stage": "end", "task_id": task_id},
            )
            if TESTING:
                # offer escape hatch for tests to come out of endless loop
                task_kwargs.pop("task_options", None)
                return dequeued_item

    async def shutdown(self) -> None:
        """
        Cleanly shutdown this worker.
        """
        self._log(
            logging.INFO,
            {
                "event": "wiji.Worker.shutdown",
                "stage": "start",
                "state": "intiating shutdown",
                "drain_duration": self.the_task.drain_duration,
            },
        )
        self.SHOULD_SHUT_DOWN = True
        if self.watchdog is not None:
            self.watchdog.stop()

        # half spent waiting for the broker, the other half just sleeping
        wait_duration = self.the_task.drain_duration / 2
        try:
            # asyncio.wait takes a python set as a first argument
            # after expiration of timeout, asyncio.wait does not cancel the task;
            # thus the broker shutdown can still continue on its own if it can.
            await asyncio.wait(
                {
                    self.the_task.the_broker.shutdown(
                        queue_name=self.the_task.queue_name, duration=wait_duration
                    )
                },
                timeout=wait_duration,
            )
        except Exception as e:
            self._log(
                logging.ERROR,
                {
                    "event": "wiji.Worker.shutdown",
                    "stage": "end",
                    "state": "calling broker shutdown error",
                    "error": str(e),
                },
            )

        # sleep so that worker can finish executing any tasks it had already dequeued.
        # we need to use asyncio.sleep so that we do not block eventloop.
        # this way, we do not prevent any other workers in the same loop from also shutting down cleanly.
        await asyncio.sleep(wait_duration)
        self.SUCCESFULLY_SHUT_DOWN = True
