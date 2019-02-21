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


class AsyncIteratorExecutor:
    """
    Converts a regular iterator into an asynchronous
    iterator, by executing the iterator in a thread.
    """

    def __init__(self, iterator, loop=None, executor=None):
        self.__iterator = iterator
        self.__loop = loop or asyncio.get_event_loop()
        self.__executor = executor

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.__loop.run_in_executor(self.__executor, next, self.__iterator, self)
        if value is self:
            raise StopAsyncIteration
        return value


class Worker:
    """
    """

    def __init__(
        self,
        tasks: typing.List[task.Task],
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

        self.loglevel = loglevel.upper()
        self.tasks = tasks

        self.Worker_id = Worker_id
        if not self.Worker_id:
            self.Worker_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))

        self.log_metadata = log_metadata
        if not self.log_metadata:
            self.log_metadata = {}
        self.log_metadata.update(
            {"Worker_id": self.Worker_id, "queue_name": "self.task.queue_name"}
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

    async def run(self, task, *task_args, **task_kwargs):
        import pdb

        pdb.set_trace()
        # run the actual queued task
        return_value = await task.async_run(*task_args, **task_kwargs)
        if task.chain:
            # enqueue the chained task using the return_value
            await task.chain.async_delay(return_value)

    async def cooler(self):
        for future in asyncio.as_completed(map(self.consume_forever, self.tasks)):
            result = await future

    async def consume_forever(
        self, task, TESTING: bool = False
    ) -> typing.Union[str, typing.Dict[typing.Any, typing.Any]]:
        """
        In loop; dequeues items from the :attr:`queue <Worker.queue>` and calls :func:`run <Worker.run>`.

        Parameters:
            TESTING: indicates whether this method is been called while running tests.
        """
        print()
        print("task:: ", task)
        print()
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
                item_to_dequeue = await task.broker.dequeue(queue_name=task.queue_name)
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
            # import pdb

            # pdb.set_trace()
            await self.run(*task_args, **task_kwargs, task=task)
            self._log(
                logging.INFO,
                {
                    "event": "xyzabc.Worker.consume_forever",
                    "stage": "end",
                    "log_id": task_log_id,
                    "task_id": task_id,
                    "item_to_dequeue": item_to_dequeue,
                },
            )
            if TESTING:
                # offer escape hatch for tests to come out of endless loop
                return item_to_dequeue
