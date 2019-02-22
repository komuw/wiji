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
from . import hooks
from . import logger
from . import ratelimiter


class Worker:
    """
    """

    def __init__(
        self,
        the_task: task.Task,
        rateLimiter=None,
        hook=None,
        Worker_id=None,
        log_handler=None,
        loglevel: str = "DEBUG",
        log_metadata=None,
    ) -> None:
        """
        """
        if loglevel.upper() not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(
                """loglevel should be one of; 'DEBUG', 'INFO', 'WARNING', 'ERROR' or 'CRITICAL'. not {0}""".format(
                    loglevel
                )
            )
        if not isinstance(the_task, (type(None), task.Task)):
            raise ValueError(
                """the_task should be of type:: None or xyzabc.task.Task You entered {0}""".format(
                    type(the_task)
                )
            )
        if not isinstance(log_metadata, (type(None), dict)):
            raise ValueError(
                """log_metadata should be of type:: None or dict. You entered {0}""".format(
                    type(log_metadata)
                )
            )

        self.loglevel = loglevel.upper()
        self.the_task = the_task

        self.Worker_id = Worker_id
        if not self.Worker_id:
            self.Worker_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))

        self.log_metadata = log_metadata
        if not self.log_metadata:
            self.log_metadata = {}
        self.log_metadata.update(
            {"Worker_id": self.Worker_id, "queue_name": self.the_task.queue_name}
        )

        self.logger = log_handler
        if not self.logger:
            self.logger = logger.SimpleBaseLogger("xyzabc.Worker")
        self.logger.bind(loglevel=self.loglevel, log_metadata=self.log_metadata)
        self._sanity_check_logger()

        self.rateLimiter = rateLimiter
        if not self.rateLimiter:
            self.rateLimiter = ratelimiter.SimpleRateLimiter(logger=self.logger)

        self.hook = hook
        if not self.hook:
            self.hook = hooks.SimpleHook(logger=self.logger)

    def _sanity_check_logger(self):
        """
        called when instantiating the Worker just to make sure the supplied
        logger can log.
        """
        try:
            self.logger.log(logging.DEBUG, {"event": "sanity_check_logger"})
        except Exception as e:
            raise e

    def _log(self, level, log_data):
        # if the supplied logger is unable to log; we move on
        try:
            self.logger.log(level, log_data)
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
        try:
            return_value = await self.the_task.async_run(*task_args, **task_kwargs)
            if self.the_task.chain:
                # enqueue the chained task using the return_value
                await self.the_task.chain.async_delay(return_value)
        except Exception as e:
            self._log(
                logging.ERROR,
                {
                    "event": "xyzabc.Worker.run",
                    "stage": "end",
                    "state": "task execution error",
                    "error": str(e),
                },
            )

    async def consume_forever(
        self, TESTING: bool = False
    ) -> typing.Union[str, typing.Dict[typing.Any, typing.Any]]:
        """
        In loop; dequeues items from the :attr:`queue <Worker.queue>` and calls :func:`run <Worker.run>`.

        Parameters:
            TESTING: indicates whether this method is been called while running tests.
        """
        retry_count = 0
        while True:
            self._log(logging.INFO, {"event": "xyzabc.Worker.consume_forever", "stage": "start"})

            try:
                # rate limit ourselves
                await self.rateLimiter.limit()
            except Exception as e:
                self._log(
                    logging.ERROR,
                    {
                        "event": "xyzabc.Worker.consume_forever",
                        "stage": "end",
                        "state": "consume_forever error",
                        "error": str(e),
                    },
                )
                continue

            try:
                item_to_dequeue = await self.the_task.broker.dequeue(
                    queue_name=self.the_task.queue_name
                )
                item_to_dequeue = json.loads(item_to_dequeue)
            except Exception as e:
                poll_queue_interval = self._retry_after(retry_count)
                retry_count += 1
                self._log(
                    logging.ERROR,
                    {
                        "event": "xyzabc.Worker.consume_forever",
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
                task_retries = item_to_dequeue["retries"]
                task_queue_name = item_to_dequeue["queue_name"]
                task_log_id = item_to_dequeue["log_id"]
                task_hook_metadata = item_to_dequeue["hook_metadata"]
                task_timelimit = item_to_dequeue["timelimit"]
                task_args = item_to_dequeue["args"]
                task_kwargs = item_to_dequeue["kwargs"]
            except KeyError as e:
                e = KeyError("enqueued message/object is missing required field:{}".format(str(e)))
                self._log(
                    logging.ERROR,
                    {
                        "event": "xyzabc.Worker.consume_forever",
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
                    "event": "xyzabc.Worker.consume_forever",
                    "stage": "end",
                    "log_id": task_log_id,
                    "task_id": task_id,
                },
            )
            if TESTING:
                # offer escape hatch for tests to come out of endless loop
                return item_to_dequeue
