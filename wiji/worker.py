import os
import uuid
import json
import random
import signal
import string
import typing
import logging
import asyncio
import datetime

from . import task
from . import hook
from . import logger
from . import protocol
from . import watchdog


class Worker:
    """
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
        self.worker_id = worker_id
        if not self.worker_id:
            self.worker_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))

        self.the_task.log_metadata.update({"worker_id": self.worker_id, "process_id": self._PID})
        self.the_task.logger.bind(
            loglevel=self.the_task.loglevel, log_metadata=self.the_task.log_metadata
        )

        self.use_watchdog = use_watchdog
        self.watchdog_duration = watchdog_duration

        self.watchdog = None
        if self.use_watchdog:
            self.watchdog = watchdog.BlockingWatchdog(
                watchdog_duration=self.watchdog_duration, task_name=self.the_task.task_name
            )

        self.SHOULD_SHUT_DOWN: bool = False
        self.SUCCESFULLY_SHUT_DOWN: bool = False

        self.the_task._sanity_check_logger(event="worker_sanity_check_logger")

    def _validate_worker_args(self, the_task, worker_id, use_watchdog, watchdog_duration):
        if not isinstance(the_task, task.Task):
            raise ValueError(
                """`the_task` should be of type:: `wiji.task.Task` You entered: {0}""".format(
                    type(the_task)
                )
            )
        if not isinstance(worker_id, (type(None), str)):
            raise ValueError(
                """`worker_id` should be of type:: `None` or `str` You entered: {0}""".format(
                    type(worker_id)
                )
            )
        if not isinstance(use_watchdog, bool):
            raise ValueError(
                """`use_watchdog` should be of type:: `bool` You entered: {0}""".format(
                    type(use_watchdog)
                )
            )
        if not isinstance(watchdog_duration, float):
            raise ValueError(
                """`watchdog_duration` should be of type:: `float` You entered: {0}""".format(
                    type(watchdog_duration)
                )
            )

    def _log(self, level, log_data):
        # if the supplied logger is unable to log; we move on
        try:
            self.the_task.logger.log(level, log_data)
        except Exception:
            pass

    @staticmethod
    def _retry_after(current_retries):
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
            return 0.5 * 60  # 0.5min
        elif current_retries == 2:
            return 1 * 60
        elif current_retries >= 6:
            return (16 * 60) + jitter  # 16 minutes + jitter
        else:
            return (60 * (2 ** current_retries)) + jitter

    async def run_task(self, *task_args, **task_kwargs):
        # run the actual queued task
        if self.watchdog is not None:
            self.watchdog.notify_alive_before()

        try:
            return_value = await self.the_task.run(*task_args, **task_kwargs)
            if self.the_task.chain:
                # enqueue the chained task using the return_value
                await self.the_task.chain.delay(return_value)
        except task.RetryError as e:
            # task is been retried
            self._log(
                logging.INFO,
                {
                    "event": "wiji.Worker.run_task",
                    "state": str(e),
                    "stage": "end",
                    "task_name": self.the_task.task_name,
                    "current_retries": self.the_task.task_options.current_retries,
                    "max_retries": self.the_task.task_options.max_retries,
                },
            )
        except Exception as e:
            self._log(
                logging.ERROR,
                {
                    "event": "wiji.Worker.run",
                    "stage": "end",
                    "state": "task execution error",
                    "error": str(e),
                },
            )
        finally:
            if self.watchdog is not None:
                self.watchdog.notify_alive_after()

    async def consume_tasks(
        self, TESTING: bool = False
    ) -> typing.Union[str, typing.Dict[typing.Any, typing.Any]]:
        """
        In loop; dequeues items from the :attr:`queue <Worker.queue>` and calls :func:`run <Worker.run>`.

        Parameters:
            TESTING: indicates whether this method is been called while running tests.
        """
        if self.watchdog is not None:
            self.watchdog.start()

        retry_count = 0
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
                return

            try:
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
                item_to_dequeue = await self.the_task.the_broker.dequeue(
                    queue_name=self.the_task.queue_name
                )
                item_to_dequeue = json.loads(item_to_dequeue)
            except Exception as e:
                poll_queue_interval = self._retry_after(retry_count)
                retry_count += 1
                self._log(
                    logging.ERROR,
                    {
                        "event": "wiji.Worker.consume_tasks",
                        "stage": "end",
                        "state": "consume_tasks error. sleeping for {0}minutes".format(
                            poll_queue_interval / 60
                        ),
                        "retry_count": retry_count,
                        "error": str(e),
                    },
                )
                await asyncio.sleep(poll_queue_interval)
                continue

            # dequeue succeded
            retry_count = 0
            try:
                task_version = item_to_dequeue["version"]
                task_id = item_to_dequeue["task_id"]
                task_eta = item_to_dequeue["eta"]
                task_current_retries = item_to_dequeue["current_retries"]
                task_max_retries = item_to_dequeue["max_retries"]
                task_log_id = item_to_dequeue["log_id"]
                task_hook_metadata = item_to_dequeue["hook_metadata"]
                task_args = item_to_dequeue["args"]
                task_kwargs = item_to_dequeue["kwargs"]
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

            now = datetime.datetime.now(tz=datetime.timezone.utc)
            if protocol.Protocol._from_isoformat(task_eta) <= now:
                await self.run_task(*task_args, **task_kwargs)
            else:
                # respect eta
                await self.the_task.delay(*task_args, **task_kwargs)
            self._log(
                logging.INFO,
                {
                    "event": "wiji.Worker.consume_tasks",
                    "stage": "end",
                    "log_id": task_log_id,
                    "task_id": task_id,
                },
            )
            if TESTING:
                # offer escape hatch for tests to come out of endless loop
                return item_to_dequeue

    async def shutdown(self):
        """
        Cleanly shutdown this worker.
        """
        self._log(
            logging.INFO,
            {
                "event": "wiji.Worker.shutdown",
                "stage": "start",
                "state": "intiating shutdown",
                "drain_duration": self.the_task.task_options.drain_duration,
            },
        )
        self.SHOULD_SHUT_DOWN = True
        if self.watchdog is not None:
            self.watchdog.stop()

        # sleep so that worker can finish executing any tasks it had already dequeued.
        # we need to use asyncio.sleep so that we do not block eventloop.
        # this way, we do not prevent any other workers in the same loop from also shutting down cleanly.
        await asyncio.sleep(self.the_task.task_options.drain_duration)
        self.SUCCESFULLY_SHUT_DOWN = True
