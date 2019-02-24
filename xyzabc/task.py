import os
import uuid
import json
import asyncio
import inspect
import datetime
import random
import string
import logging

from . import broker
from . import ratelimiter
from . import hook
from . import logger


class Task:
    """
    call it as:
        Task()(33,"hello", name="komu")

    usage:
        broker = xyzabc.broker.SimpleBroker()
        task = Task(
                broker=broker,
                queue_name="PrintQueue",
                eta=60,
                retries=3,
                log_id="myLogID",
                hook_metadata='{"email": "example@example.com"}',
            )
        task.delay(33, "hello", name="komu")
    
    You can also chain things as:
        task1 = xyzabc.task.Task()
        task2 = xyzabc.task.Task(chain=task1)
        task3 = xyzabc.task.Task(chain=task2)
    """

    def __init__(
        self,
        the_broker: broker.BaseBroker,
        queue_name,
        eta,
        retries,
        log_id,
        hook_metadata,
        task_name=None,
        task_id=None,
        chain=None,
        the_hook=None,
        rateLimiter=None,
        loglevel: str = "DEBUG",
        log_metadata=None,
        log_handler=None,
    ) -> None:
        self._validate_task_args(
            the_broker=the_broker,
            queue_name=queue_name,
            eta=eta,
            retries=retries,
            log_id=log_id,
            hook_metadata=hook_metadata,
            task_name=task_name,
            task_id=task_id,
            chain=chain,
            the_hook=the_hook,
            rateLimiter=rateLimiter,
            loglevel=loglevel,
            log_metadata=log_metadata,
            log_handler=log_handler,
        )

        self.the_broker = the_broker
        self.queue_name = queue_name
        self.eta = eta
        self.retries = retries
        self.log_id = log_id
        self.hook_metadata = hook_metadata
        self.task_name = task_name
        self.task_id = task_id
        self.chain = chain
        self.loglevel = loglevel.upper()

        self.task_name = task_name
        if not self.task_name:
            self.task_name = self.__class__.__name__

        self.task_id = task_id
        if not self.task_id:
            self.task_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))

        self.log_metadata = log_metadata
        if not self.log_metadata:
            self.log_metadata = {}
        self.log_metadata.update(
            {
                "task_name": self.task_name,
                "task_id": self.task_id,
                "log_id": self.log_id,
                "queue_name": self.queue_name,
            }
        )

        self.logger = log_handler
        if not self.logger:
            self.logger = logger.SimpleBaseLogger("xyzabc.Task")
        self.logger.bind(loglevel=self.loglevel, log_metadata=self.log_metadata)
        self._sanity_check_logger(event="task_sanity_check_logger")

        self.the_hook = the_hook
        if not self.the_hook:
            self.the_hook = hook.SimpleHook(logger=self.logger)

        self.rateLimiter = rateLimiter
        if not self.rateLimiter:
            self.rateLimiter = ratelimiter.SimpleRateLimiter(logger=self.logger)

    def __or__(self, other):
        """
        Operator Overloading is bad.
        It should die a swift death.

        This allows someone to do:
            task1 = xyzabc.task.Task()
            task2 = xyzabc.task.Task()
            task3 = xyzabc.task.Task()

            task1 | task2 | task3
        """
        self.chain = other
        return other

    async def __call__(self, *args, **kwargs):
        await self.async_run(*args, **kwargs)

    def _validate_task_args(
        self,
        the_broker,
        queue_name,
        eta,
        retries,
        log_id,
        hook_metadata,
        task_name,
        task_id,
        chain,
        the_hook,
        rateLimiter,
        loglevel,
        log_metadata,
        log_handler,
    ):
        if not isinstance(the_broker, (type(None), broker.BaseBroker)):
            raise ValueError(
                """the_broker should be of type:: None or xyzabc.broker.BaseBroker You entered: {0}""".format(
                    type(the_broker)
                )
            )
        if not isinstance(queue_name, str):
            raise ValueError(
                """`queue_name` should be of type:: `str` You entered: {0}""".format(
                    type(queue_name)
                )
            )
        if not isinstance(eta, float):
            raise ValueError(
                """`eta` should be of type:: `float` You entered: {0}""".format(type(eta))
            )
        if not isinstance(retries, int):
            raise ValueError(
                """`retries` should be of type:: `int` You entered: {0}""".format(type(retries))
            )
        if not isinstance(log_id, str):
            raise ValueError(
                """`log_id` should be of type:: `str` You entered: {0}""".format(type(log_id))
            )
        if not isinstance(hook_metadata, (type(None), str)):
            raise ValueError(
                """`hook_metadata` should be of type:: `None` or `str` You entered: {0}""".format(
                    type(hook_metadata)
                )
            )
        if not isinstance(task_name, (type(None), str)):
            raise ValueError(
                """`task_name` should be of type:: `None` or `str` You entered: {0}""".format(
                    type(task_name)
                )
            )
        if not isinstance(task_id, (type(None), str)):
            raise ValueError(
                """`task_id` should be of type:: `None` or `str` You entered: {0}""".format(
                    type(task_id)
                )
            )
        if not isinstance(chain, (type(None), Task)):
            raise ValueError(
                """`chain` should be of type:: `None` or `xyzabc.task.Task` You entered: {0}""".format(
                    type(chain)
                )
            )
        if not isinstance(the_hook, (type(None), hook.BaseHook)):
            raise ValueError(
                """`the_hook` should be of type:: `None` or `xyzabc.hook.BaseHook` You entered: {0}""".format(
                    type(the_hook)
                )
            )
        if not isinstance(rateLimiter, (type(None), ratelimiter.BaseRateLimiter)):
            raise ValueError(
                """`rateLimiter` should be of type:: `None` or `xyzabc.ratelimiter.BaseRateLimiter` You entered: {0}""".format(
                    type(rateLimiter)
                )
            )
        if not isinstance(log_handler, (type(None), logger.BaseLogger)):
            raise ValueError(
                """`log_handler` should be of type:: `None` or `xyzabc.logger.BaseLogger` You entered: {0}""".format(
                    type(log_handler)
                )
            )
        if loglevel.upper() not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(
                """`loglevel` should be one of; 'DEBUG', 'INFO', 'WARNING', 'ERROR' or 'CRITICAL'. You entered: {0}""".format(
                    loglevel
                )
            )
        if not isinstance(log_metadata, (type(None), dict)):
            raise ValueError(
                """`log_metadata` should be of type:: `None` or `dict` You entered: {0}""".format(
                    type(log_metadata)
                )
            )

        if not asyncio.iscoroutinefunction(self.async_run):
            raise ValueError(
                "The method: `async_run` of a class derived from: `xyzabc.task.Task` should be a python coroutine."
                "\nHint: did you forget to define the method using `async def` syntax?"
            )
        if not inspect.iscoroutinefunction(self.async_run):
            raise ValueError(
                "The method: `async_run` of a class derived from: `xyzabc.task.Task` should be a python coroutine."
                "\nHint: did you forget to define the method using `async def` syntax?"
            )

    def _sanity_check_logger(self, event):
        """
        Called when we want to make sure the supplied logger can log.
        This usually happens when we are instantiating a xyzabc.Task or a xyzabc.Worker
        """
        try:
            self.logger.log(logging.DEBUG, {"event": event})
        except Exception as e:
            raise e

    def _log(self, level, log_data):
        # if the supplied logger is unable to log; we move on
        try:
            self.logger.log(level, log_data)
        except Exception:
            pass

    async def async_run(self, *args, **kwargs):
        raise NotImplementedError("run method must be implemented.")

    def blocking_run(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_run(*args, **kwargs))

    async def async_delay(self, *args, **kwargs):
        """
        Parameters:
            args: The positional arguments to pass on to the task.
            kwargs: The keyword arguments to pass on to the task.
        """
        # Queue this to queue
        class_name: str = self.__class__.__name__

        eta = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.eta)
        protocol = {
            "version": 1,
            "task_id": str(uuid.uuid4()),
            "eta": eta.isoformat(),
            "retries": self.retries,
            "queue_name": self.queue_name,
            "log_id": self.log_id,
            "hook_metadata": self.hook_metadata,
            "timelimit": 1800,
            "args": args,
            "kwargs": kwargs,
        }

        protocol_json = json.dumps(protocol)
        await self.the_broker.enqueue(item=protocol_json, queue_name=self.queue_name)

    def blocking_delay(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_delay(*args, **kwargs))


class _watchDogTask(Task):
    def __init__(
        self,
        eta=0.00,
        retries=0,
        log_id="log_id",
        hook_metadata="hook_metadata",
        the_broker=broker.SimpleBroker(),
        queue_name="WatchDogTask_Queue",
        task_name=None,
        task_id=None,
        chain=None,
        the_hook=None,
        rateLimiter=None,
        loglevel: str = "DEBUG",
        log_metadata=None,
        log_handler=None,
    ):
        # we should always use in-memory broker for watchdog task
        super(_watchDogTask, self).__init__(
            the_broker, queue_name, eta, retries, log_id, hook_metadata
        )
        # Enables task watchdog. This will spawn a separate thread that will check if any tasks are blocked,
        # and if so will notify you and print the stack traces of all threads to show exactly where the program is blocked.
        self.use_watchdog: bool = True

        # The number of seconds the watchdog will wait before notifying that the main thread is blocked.
        self.watchdog_timeout: float = 0.1  # 100 millisecond

        if not isinstance(self.use_watchdog, bool):
            raise ValueError(
                """`use_watchdog` should be of type:: `bool` You entered: {0}""".format(
                    type(self.use_watchdog)
                )
            )
        if not isinstance(self.watchdog_timeout, float):
            raise ValueError(
                """`watchdog_timeout` should be of type:: `float` You entered: {0}""".format(
                    type(self.watchdog_timeout)
                )
            )

    async def async_run(self):
        self._log(
            logging.DEBUG,
            {
                "event": "xyzabc.WatchDogTask.async_run",
                "state": "watchdog_run",
                "task_name": self.task_name,
                "task_id": self.task_id,
            },
        )
        await asyncio.sleep(self.watchdog_timeout / 2)


WatchDogTask = _watchDogTask()
