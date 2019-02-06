import os
import uuid
import json
import random
import string
import typing
import logging
import asyncio
import datetime

from . import broker
from . import task
from . import hooks
from . import logger
from . import ratelimiter


class Worker:
    """
    """

    def __init__(
        self,
        async_loop: asyncio.events.AbstractEventLoop,
        broker: broker.BaseBroker,
        queue_name_and_task: typing.Dict[str, task.Task],
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
        if not isinstance(log_metadata, (type(None), dict)):
            raise ValueError(
                """log_metadata should be of type:: None or dict. You entered {0}""".format(
                    type(log_metadata)
                )
            )

        self.async_loop = async_loop
        self.loglevel = loglevel.upper()
        self.broker = broker
        self.queue_name_and_task = queue_name_and_task

        self.Worker_id = Worker_id
        if not self.Worker_id:
            self.Worker_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))

        self.log_metadata = log_metadata
        if not self.log_metadata:
            self.log_metadata = {}
        self.log_metadata.update({"Worker_id": self.Worker_id})

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
        retries will happen in this sequence;
        1min, 2min, 4min, 8min, 16min, 32min, 16min, 16min, 16min ...
        """
        # TODO:
        # 1. give users ability to bring their own retry algorithms.
        # 2. add jitter
        if current_retries < 0:
            current_retries = 0
        if current_retries >= 6:
            return 60 * 16  # 16 minutes
        else:
            return 60 * (1 * (2 ** current_retries))

    async def run(self, *task_args, **task_kwargs):
        # run the actual queued task
        xyzabc_task_Unique_name = task_kwargs.pop("xyzabc_task_Unique_name")
        await xyzabc_task_Unique_name(*task_args, **task_kwargs)

    async def cooler(self):
        import pdb;pdb.set_trace()
        for queue_name in self.queue_name_and_task:
            print("queue_name::", queue_name)
            await self.consume_forever(queue_name)

    async def consume_forever(
        self, queue_name: str, TESTING: bool = False
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
                item_to_dequeue = await self.broker.dequeue(queue_name=queue_name)
                item_to_dequeue = json.loads(item_to_dequeue)
            except Exception as e:
                retry_count += 1
                poll_queue_interval = self._retry_after(retry_count)
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
                task_file_name = item_to_dequeue["file_name"]
                task_class_path = item_to_dequeue["class_path"]
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

            task = self.queue_name_and_task[queue_name]
            task_kwargs.update({"xyzabc_task_Unique_name": task})
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
