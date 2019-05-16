import abc
import time
import enum
import uuid
import string
import random
import typing
import asyncio
import logging
import inspect

from . import hook
from . import logger
from . import broker
from . import protocol
from . import ratelimiter


# TODO: disambiguate which attributes should be in TaskOptions class
# and which ones should be in Task class.
# looks like the attributes that should be in TaskOptions class are the ones
# that have to do with the task as it is been called as opposed to when it is been declared.


class WijiMaxRetriesExceededError(Exception):
    """
    The tasks max_retries count has been exceeded.
    """

    pass


class TaskQueueingError(Exception):
    """
    raised if `wiji` is unable to publish to the broker for any reason.
    """

    pass


@enum.unique
class TaskState(enum.Enum):
    QUEUEING: int = 1
    QUEUED: int = 2
    DEQUEUED: int = 3
    EXECUTING: int = 4
    EXECUTED: int = 5


class TaskOptions:
    def __init__(
        self, eta: float = 0.00, max_retries: int = 0, hook_metadata: typing.Union[None, str] = None
    ):
        """
        this are the options that you can supply when calling `task.delay`
        ie, they are the config options that only apply to that `task.delay` invocation eg `eta`

        If a config option applies to the `Task` instance itself, then it should not be in this class eg `drain_duration`

        Note that a `Task` class does not have a `TaskOptions` attribute at creation time, it gets one when `task.delay` is first called.
        """
        self._validate_task_options_args(
            eta=eta, max_retries=max_retries, hook_metadata=hook_metadata
        )
        if eta < 0.00:
            eta = 0.00
        self.eta = protocol.Protocol._eta_to_isoformat(eta=eta)
        self.task_id = ""
        self.current_retries: int = 0
        self.max_retries = max_retries
        if self.max_retries < 0:
            self.max_retries = 0

        if hook_metadata is not None:
            self.hook_metadata = hook_metadata
        else:
            self.hook_metadata = ""

        self.args: tuple = ()
        self.kwargs: dict = {}

    def __str__(self):
        return str(self.__dict__)

    def _validate_task_options_args(
        self, eta: float, max_retries: int, hook_metadata: typing.Union[None, str]
    ) -> None:
        if not isinstance(eta, float):
            raise ValueError(
                """`eta` should be of type:: `float` You entered: {0}""".format(type(eta))
            )
        if not isinstance(max_retries, int):
            raise ValueError(
                """`max_retries` should be of type:: `int` You entered: {0}""".format(
                    type(max_retries)
                )
            )
        if not isinstance(hook_metadata, (type(None), str)):
            raise ValueError(
                """`hook_metadata` should be of type:: `None` or `str` You entered: {0}""".format(
                    type(hook_metadata)
                )
            )

    def dictsy(self):
        return {
            "eta": self.eta,
            "task_id": self.task_id,
            "current_retries": self.current_retries,
            "max_retries": self.max_retries,
            "hook_metadata": self.hook_metadata,
            "args": self.args,
            "kwargs": self.kwargs,
        }


class Task(abc.ABC):
    """
    usage:
        class AdderTask(wiji.task.Task):
            the_broker = wiji.broker.InMemoryBroker()
            queue_name = "AdderTaskQueue"

            async def run(self, a, b):
                result = a + b
                return result

        task = AdderTask()
        await task.delay(33, 14)
    """

    the_broker: broker.BaseBroker
    queue_name: str
    task_name: typing.Union[None, str] = None

    # chain lets us link together `Tasks` so that one is called after the other, forming a chain.
    chain: typing.Union[None, typing.Type["Task"]] = None

    the_hook: typing.Union[None, hook.BaseHook] = None
    the_ratelimiter: typing.Union[None, ratelimiter.BaseRateLimiter] = None

    # `drain_duration` is the duration(in seconds) that a worker should wait
    # after getting a termination signal(SIGTERM, SIGQUIT etc).
    # during this duration, the worker does not consumer anymore tasks from the broker,
    # the worker will continue executing any tasks that it had already dequeued from the broker.
    # a simple way of choosing a value to set is:
    # drain_duration = time_taken_to_run_this_task + 1.00
    # eg: if your task is making a network call that lasts 30seconds,
    # thus; drain_duration = 30 + 1.00
    # the default value is 10.00 seconds.
    # mainly because that is also the default value of the process supervisor: `supervisord`
    drain_duration: float = 10.0

    loglevel: str = "INFO"
    log_metadata: typing.Union[None, dict] = None
    log_handler: typing.Union[None, logger.BaseLogger] = None

    def __init__(self,) -> None:
        self._debug_task_name = self.__class__.__name__
        self._validate_task_args()
        self.loglevel = self.loglevel.upper()

        # we can have tasks that have no chains
        self.the_chain: typing.Union[None, "Task"] = None
        if self.chain is not None:
            assert inspect.isclass(
                self.chain
            ), "Task: {0}. `chain` should be a class and NOT a class instance".format(
                self._debug_task_name
            )
            assert issubclass(
                self.chain, Task
            ), "Task: {0}. `chain` should be a subclass of:: `wiji.task.Task`".format(
                self._debug_task_name
            )
            assert callable(self.chain), "Task: {0}. `chain` should be a callable".format(
                self._debug_task_name
            )
            # https://github.com/PyCQA/pylint/issues/1493
            self.the_chain: "Task" = self.chain()  # pylint: disable=E1102

        if self.task_name is not None:
            self.task_name = self.task_name
        else:
            self.task_name = self.__class__.__name__

        if self.log_metadata is not None:
            self.log_metadata = self.log_metadata
        else:
            self.log_metadata = {}
        self.log_metadata.update({"task_name": self.task_name, "queue_name": self.queue_name})

        if self.log_handler is not None:
            self.logger = self.log_handler
        else:
            self.logger = logger.SimpleLogger(
                "wiji.Task.task_name={0}.{1}".format(
                    self.task_name,
                    "".join(random.choices(string.ascii_lowercase + string.digits, k=5)),
                )
            )
        self.logger.bind(level=self.loglevel, log_metadata=self.log_metadata)
        self._sanity_check_logger(event="task_sanity_check_logger")

        if self.the_hook is not None:
            self.the_hook = self.the_hook
        else:
            self.the_hook = hook.SimpleHook(log_handler=self.logger)

        if self.the_ratelimiter is not None:
            self.the_ratelimiter = self.the_ratelimiter
        else:
            self.the_ratelimiter = ratelimiter.SimpleRateLimiter(log_handler=self.logger)

        self._checked_broker: bool = False
        self._RETRYING: bool = False

    async def __call__(self, *args, **kwargs):
        return await self.run(*args, **kwargs)

    def __str__(self):
        return str(
            {
                "task_name": self.task_name,
                "the_broker": self.the_broker,
                "queue_name": self.queue_name,
                "chain": self.the_chain,
            }
        )

    def _validate_task_args(self,) -> None:
        if not hasattr(self, "the_broker"):
            raise ValueError(
                "Task: {0} should have attribute `the_broker`".format(self._debug_task_name)
            )
        if not isinstance(self.the_broker, broker.BaseBroker):
            raise ValueError(
                "Task: {0}. `the_broker` should be of type:: `wiji.broker.BaseBroker` You entered: {0}".format(
                    type(self._debug_task_name, self.the_broker)
                )
            )

        if not hasattr(self, "queue_name"):
            raise ValueError(
                "Task: {0} should have attribute `queue_name`".format(self._debug_task_name)
            )
        if not isinstance(self.queue_name, str):
            raise ValueError(
                "Task: {0}. `queue_name` should be of type:: `str` You entered: {1}".format(
                    self._debug_task_name, type(self.queue_name)
                )
            )

        if not hasattr(self, "task_name"):
            raise ValueError(
                "Task: {0} should have attribute `task_name`".format(self._debug_task_name)
            )
        if not isinstance(self.task_name, (type(None), str)):
            raise ValueError(
                "Task: {0}. `task_name` should be of type:: `None` or `str` You entered: {1}".format(
                    self._debug_task_name, type(self.task_name)
                )
            )

        if not hasattr(self, "chain"):
            raise ValueError(
                "Task: {0} should have attribute `chain`".format(self._debug_task_name)
            )
        if self.chain:
            if not inspect.isclass(self.chain):
                raise ValueError(
                    "Task: {0}. `chain` should be a class and NOT a class instance".format(
                        self._debug_task_name
                    )
                )
            if not issubclass(self.chain, Task):
                raise ValueError(
                    "Task: {0}. `chain` should be a subclass of:: `wiji.task.Task`".format(
                        self._debug_task_name
                    )
                )

        if not hasattr(self, "the_hook"):
            raise ValueError(
                "Task: {0} should have attribute `the_hook`".format(self._debug_task_name)
            )
        if not isinstance(self.the_hook, (type(None), hook.BaseHook)):
            raise ValueError(
                "Task: {0}. `the_hook` should be of type:: `None` or `wiji.hook.BaseHook` You entered: {1}".format(
                    self._debug_task_name, type(self.the_hook)
                )
            )

        if not hasattr(self, "the_ratelimiter"):
            raise ValueError(
                "Task: {0} hould have attribute `the_ratelimiter`".format(self._debug_task_name)
            )
        if not isinstance(self.the_ratelimiter, (type(None), ratelimiter.BaseRateLimiter)):
            raise ValueError(
                "Task: {0}. `the_ratelimiter` should be of type:: `None` or `wiji.ratelimiter.BaseRateLimiter` You entered: {1}".format(
                    self._debug_task_name, type(self.the_ratelimiter)
                )
            )

        if not hasattr(self, "drain_duration"):
            raise ValueError(
                "Task: {0} should have attribute `drain_duration`".format(self._debug_task_name)
            )
        if not isinstance(self.drain_duration, float):
            raise ValueError(
                "Task: {0}. `drain_duration` should be of type:: `float` You entered: {1}".format(
                    self._debug_task_name, type(self.drain_duration)
                )
            )

        if not hasattr(self, "log_handler"):
            raise ValueError(
                "Task: {0} should have attribute `log_handler`".format(self._debug_task_name)
            )
        if not isinstance(self.log_handler, (type(None), logger.BaseLogger)):
            raise ValueError(
                "Task: {0}. `log_handler` should be of type:: `None` or `wiji.logger.BaseLogger` You entered: {1}".format(
                    self._debug_task_name, type(self.log_handler)
                )
            )

        if not hasattr(self, "loglevel"):
            raise ValueError(
                "Task: {0} should have attribute `loglevel`".format(self._debug_task_name)
            )
        if self.loglevel.upper() not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(
                "Task: {0}. `loglevel` should be one of; 'DEBUG', 'INFO', 'WARNING', 'ERROR' or 'CRITICAL'. You entered: {1}".format(
                    self._debug_task_name, self.loglevel
                )
            )

        if not hasattr(self, "log_metadata"):
            raise ValueError(
                "Task: {0} should have attribute `log_metadata`".format(self._debug_task_name)
            )
        if not isinstance(self.log_metadata, (type(None), dict)):
            raise ValueError(
                "Task: {0}. `log_metadata` should be of type:: `None` or `dict` You entered: {1}".format(
                    self._debug_task_name, type(self.log_metadata)
                )
            )

        if not asyncio.iscoroutinefunction(self.run):
            raise ValueError(
                "Task: {0}. The method `run` of a class derived from `wiji.task.Task` should be a python coroutine.".format(
                    self._debug_task_name
                )
            )
        if not inspect.iscoroutinefunction(self.run):
            raise ValueError(
                "Task: {0}. The method `run` of a class derived from `wiji.task.Task` should be a python coroutine.".format(
                    self._debug_task_name
                )
            )
        if not asyncio.iscoroutinefunction(self.delay):
            raise ValueError(
                "Task: {0}. The method `delay` of a class derived from `wiji.task.Task` should be a python coroutine.".format(
                    self._debug_task_name
                )
            )
        if not inspect.iscoroutinefunction(self.delay):
            raise ValueError(
                "Task: {0}. The method `delay` of a class derived from `wiji.task.Task` should be a python coroutine.".format(
                    self._debug_task_name
                )
            )
        if not asyncio.iscoroutinefunction(self.retry):
            raise ValueError(
                "Task: {0}. The method `retry` of a class derived from `wiji.task.Task` should be a python coroutine.".format(
                    self._debug_task_name
                )
            )
        if not inspect.iscoroutinefunction(self.retry):
            raise ValueError(
                "Task: {0}. The method `retry` of a class derived from `wiji.task.Task` should be a python coroutine.".format(
                    self._debug_task_name
                )
            )

    def _sanity_check_logger(self, event: str) -> None:
        """
        Called when we want to make sure the supplied logger can log.
        This usually happens when we are instantiating a wiji.Task or a wiji.Worker
        """
        try:
            self.logger.log(logging.DEBUG, {"event": event})
        except Exception as e:
            raise e

    def _log(self, level: typing.Union[str, int], log_data: dict) -> None:
        # if the supplied logger is unable to log; we move on
        try:
            self.logger.log(level, log_data)
        except Exception:
            pass

    async def _broker_check(self, from_worker: bool) -> None:
        try:
            await self.the_broker.check(queue_name=self.queue_name)
            if not from_worker:
                self._checked_broker = True
        except Exception as e:
            event = "wiji.Task.delay"
            if from_worker:
                event = "wiji.Worker.consume_tasks"
            self._log(
                logging.ERROR,
                {"event": event, "stage": "end", "state": "check broker failed", "error": str(e)},
            )
            # exit with error
            raise ValueError(
                "Task: {0}. The broker failed check request.".format(self._debug_task_name)
            ) from e

    async def _notify_ratelimiter(
        self,
        task_id: str,
        state: TaskState,
        queuing_duration: typing.Union[None, typing.Dict[str, float]] = None,
        queuing_exception: typing.Union[None, Exception] = None,
        execution_duration: typing.Union[None, typing.Dict[str, float]] = None,
        execution_exception: typing.Union[None, Exception] = None,
        return_value: typing.Union[None, typing.Any] = None,
    ) -> None:
        try:
            if typing.TYPE_CHECKING:
                assert isinstance(self.the_ratelimiter, ratelimiter.BaseRateLimiter)
                assert isinstance(self.task_name, str)
            await self.the_ratelimiter.notify(
                task_id=task_id,
                task_name=self.task_name,
                queue_name=self.queue_name,
                state=state,
                queuing_duration=queuing_duration,
                queuing_exception=queuing_exception,
                execution_duration=execution_duration,
                execution_exception=execution_exception,
                return_value=return_value,
            )
        except Exception as e:
            self._log(
                logging.ERROR,
                {
                    "event": "wiji.Task._notify_ratelimiter",
                    "stage": "end",
                    "state": "task ratelimiter error",
                    "error": str(e),
                },
            )

    async def _notify_hook(
        self,
        task_id: str,
        state: TaskState,
        hook_metadata: str,
        queuing_duration: typing.Union[None, typing.Dict[str, float]] = None,
        queuing_exception: typing.Union[None, Exception] = None,
        execution_duration: typing.Union[None, typing.Dict[str, float]] = None,
        execution_exception: typing.Union[None, Exception] = None,
        return_value: typing.Union[None, typing.Any] = None,
    ) -> None:
        try:
            if typing.TYPE_CHECKING:
                # make mypy happy: https://github.com/python/mypy/issues/4805
                assert isinstance(self.task_name, str)
                assert isinstance(self.the_hook, hook.BaseHook)
            await self.the_hook.notify(
                task_name=self.task_name,
                queue_name=self.queue_name,
                task_id=task_id,
                state=state,
                hook_metadata=hook_metadata,
                queuing_duration=queuing_duration,
                queuing_exception=queuing_exception,
                execution_duration=execution_duration,
                execution_exception=execution_exception,
                return_value=return_value,
            )
        except Exception as e:
            self._log(
                logging.ERROR,
                {
                    "event": "wiji.Task._notify_hook",
                    "stage": "end",
                    "state": "task hook error",
                    "error": str(e),
                },
            )

    @abc.abstractmethod
    async def run(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        raise NotImplementedError(
            "Task: {0}. `run` method must be implemented.".format(self._debug_task_name)
        )

    async def delay(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """
        Parameters:
            args: The positional arguments to pass on to the task.
            kwargs: The keyword arguments to pass on to the task.
        """
        # _get_task_options should be called first
        task_options = self._get_task_options(*args, **kwargs)
        args = task_options.args
        kwargs = task_options.kwargs

        self._validate_delay_args(*args, **kwargs)
        self._type_check(self.run, *args, **kwargs)
        if not self._checked_broker:
            await self._broker_check(from_worker=False)

        await self._notify_hook(
            task_id=task_options.task_id,
            state=TaskState.QUEUEING,
            hook_metadata=task_options.hook_metadata,
        )

        queuing_exception = None
        thread_time_start = time.thread_time()
        perf_counter_start = time.perf_counter()
        monotonic_start = time.monotonic()
        process_time_start = time.process_time()
        try:
            proto = protocol.Protocol(version=1, task_options=task_options)
            await self.the_broker.enqueue(queue_name=self.queue_name, item=proto.json())
        except TypeError as e:
            self._log(logging.ERROR, {"event": "wiji.Task.delay", "stage": "end", "error": str(e)})
            raise TypeError(
                "Task: {0}. All the task arguments passed into `delay` should be JSON serializable.".format(
                    self._debug_task_name
                )
            ) from e
        except Exception as e:
            queuing_exception = e
            self._log(
                logging.ERROR,
                {
                    "event": "wiji.Task.delay",
                    "stage": "end",
                    "state": "task queueing error",
                    "error": str(e),
                },
            )
            raise TaskQueueingError(
                "Task: {0}. publishing to the broker failed.".format(self._debug_task_name)
            ) from e
        finally:
            thread_time_end = time.thread_time()
            perf_counter_end = time.perf_counter()
            monotonic_end = time.monotonic()
            process_time_end = time.process_time()
            queuing_duration = {
                "thread_time": float("{0:.4f}".format(thread_time_end - thread_time_start)),
                "perf_counter": float("{0:.4f}".format(perf_counter_end - perf_counter_start)),
                "monotonic": float("{0:.4f}".format(monotonic_end - monotonic_start)),
                "process_time": float("{0:.4f}".format(process_time_end - process_time_start)),
            }
            # this cannot raise an error since the method handles that error
            await self._notify_hook(
                task_id=task_options.task_id,
                state=TaskState.QUEUED,
                hook_metadata=task_options.hook_metadata,
                queuing_duration=queuing_duration,
                queuing_exception=queuing_exception,
            )
            await self._notify_ratelimiter(
                task_id=task_options.task_id,
                state=TaskState.QUEUED,
                queuing_duration=queuing_duration,
                queuing_exception=queuing_exception,
            )

    def synchronous_delay(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()

        loop.run_until_complete(self.delay(*args, **kwargs))

    async def retry(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """
        Parameters:
            args: The positional arguments to pass on to the task.
            kwargs: The keyword arguments to pass on to the task.

        Raises:
            WijiMaxRetriesExceededError: the task exceeded its max_retries count.

        This method takes the same parameters as the `delay` method.
        It also behaves the same as `delay`
        """
        # _get_task_options should be called first
        self._get_task_options(*args, **kwargs)
        self._validate_delay_args(*args, **kwargs)

        if self.current_retries >= self.max_retries:
            self._RETRYING = False
            raise WijiMaxRetriesExceededError(
                "Task: {0}. The task has reached its max_retries count of: {1}".format(
                    self._debug_task_name, self.max_retries
                )
            )

        self.current_retries += 1
        await self.delay(*args, **kwargs)

        self._RETRYING = True

    def _validate_delay_args(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        for a in args:
            if isinstance(a, TaskOptions):
                raise ValueError(
                    "Task: {0}. You cannot use a value of type `wiji.task.TaskOptions` as a normal argument.".format(
                        self._debug_task_name
                    )
                )

    @staticmethod
    def _type_check(func, *args: typing.Any, **kwargs: typing.Any) -> inspect.BoundArguments:
        """
        Check that `delay` is called with right arguments/signature.
        ie, the right arguments for the user implemented `run` method.

        if you have a func like:
            def foo(a, b, *args, c, d=10, **kwargs):
                pass
        you can type-check like:
            Task._type_check(foo, 1, 4)
        """
        sig = inspect.signature(func)
        return sig.bind(*args, **kwargs)

    def _get_task_options(self, *args: typing.Any, **kwargs: typing.Any) -> TaskOptions:
        task_options = None
        for k, v in list(kwargs.items()):
            if isinstance(v, TaskOptions):
                task_options = v
                kwargs.pop(k)

        if not task_options:
            # create a default task_options
            task_options = TaskOptions()

        # every invocation of `my_task.delay()` is counted as unique and
        # should have a unique task_id even if it is a retry of a previous request
        task_options.task_id = str(uuid.uuid4())
        task_options.args = args
        task_options.kwargs = kwargs

        # set retries
        self.max_retries = task_options.max_retries
        if not hasattr(self, "current_retries"):
            # if `current_retries` exists, don't ovveride it
            self.current_retries = task_options.current_retries

        return task_options


class _watchdogTask(Task):
    """
    This is a task that runs in the MainThread(as every other task).
    Its job is to start a new thread(Thread-<wiji_watchdog>) and communicate with it.
    That new thread will log a stack-trace if it detects any blocking calls(IO-bound, CPU-bound or otherwise) running on the MainThread.
    That trace is meant to help users of `wiji` be able to fix their applications.

    This task is always scheduled in the in-memory broker(`wiji.broker.InMemoryBroker`).
    """

    the_broker = broker.InMemoryBroker()
    queue_name: str = "__WatchDogTaskQueue__"
    loglevel = "WARNING"

    async def run(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        self._log(
            logging.DEBUG,
            {
                "event": "wiji.WatchDogTask.run",
                "state": "watchdog_run",
                "task_name": self.task_name,
            },
        )
        await asyncio.sleep(0.1 / 1.5)


WatchDogTask: _watchdogTask = _watchdogTask()
