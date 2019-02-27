import os
import uuid
import json
import random
import string
import typing
import logging
import asyncio
import datetime

from . import task
from . import hook
from . import logger

from . import _watchdog


class Worker:
    """
    """

    def __init__(self, the_task: task.Task, worker_id=None) -> None:
        """
        """
        self._validate_worker_args(the_task=the_task, worker_id=worker_id)

        self.the_task = the_task
        self.worker_id = worker_id
        if not self.worker_id:
            self.worker_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))

        self.the_task.log_metadata.update({"worker_id": self.worker_id})
        self.the_task.logger.bind(
            loglevel=self.the_task.loglevel, log_metadata=self.the_task.log_metadata
        )

        self.use_watchdog = False
        self.watchdog = None
        if hasattr(self.the_task, "use_watchdog") and hasattr(self.the_task, "watchdog_timeout"):
            self.use_watchdog = self.the_task.use_watchdog
            self.watchdog_timeout = self.the_task.watchdog_timeout
            self.watchdog = _watchdog._BlocingWatchdog(
                timeout=self.watchdog_timeout, task_name=self.the_task.task_name
            )

        self.the_task._sanity_check_logger(event="worker_sanity_check_logger")

    def _validate_worker_args(self, the_task, worker_id):
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

    async def run(self, *task_args, **task_kwargs):
        # run the actual queued task
        if self.watchdog is not None:
            self.watchdog.notify_alive_before()

        try:
            return_value = await self.the_task.async_run(*task_args, **task_kwargs)
            if self.the_task.chain:
                # enqueue the chained task using the return_value
                await self.the_task.chain.async_delay(return_value)
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

    async def consume_forever(
        self, TESTING: bool = False
    ) -> typing.Union[str, typing.Dict[typing.Any, typing.Any]]:
        """
        In loop; dequeues items from the :attr:`queue <Worker.queue>` and calls :func:`run <Worker.run>`.

        Parameters:
            TESTING: indicates whether this method is been called while running tests.
        """
        if self.watchdog is not None:
            self.watchdog.start()
            # queue the first watchdog task
            await self.the_task.async_delay()

        retry_count = 0
        while True:

            self._log(logging.INFO, {"event": "wiji.Worker.consume_forever", "stage": "start"})

            try:
                # rate limit ourselves
                await self.the_task.rateLimiter.limit()
            except Exception as e:
                self._log(
                    logging.ERROR,
                    {
                        "event": "wiji.Worker.consume_forever",
                        "stage": "end",
                        "state": "consume_forever error",
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
                        "event": "wiji.Worker.consume_forever",
                        "stage": "end",
                        "state": "consume_forever error. sleeping for {0}minutes".format(
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
                e = KeyError("enqueued message/object is missing required field:{}".format(str(e)))
                self._log(
                    logging.ERROR,
                    {
                        "event": "wiji.Worker.consume_forever",
                        "stage": "end",
                        "state": "consume_forever error",
                        "error": str(e),
                    },
                )
                continue

            await self.run(*task_args, **task_kwargs)
            self._log(
                logging.INFO,
                {
                    "event": "wiji.Worker.consume_forever",
                    "stage": "end",
                    "log_id": task_log_id,
                    "task_id": task_id,
                },
            )
            if TESTING:
                # offer escape hatch for tests to come out of endless loop
                return item_to_dequeue

    def shutdown(self):
        """
        Cleanly shutdown worker.
        TODO: see, https://github.com/komuw/wiji/issues/2
        """
        if self.watchdog is not None:
            self.watchdog.stop()