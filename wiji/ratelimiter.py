import abc
import time
import typing
import asyncio
import logging

from . import task
from . import logger


# TODO: rate limiting should take into account number of succesful task executions(based on whether they raised an exception or not)
# and also the number of failures.
# We should also pass in the task_name, and exception raised
# This will enable users of wiji to come up with better rate limiting algos.
# So: we need to make the BaseRateLimiter accept this more arguments.
# We also need to make SimpleRateLimiter a bit smarter and take this new args in effect(or maybe not[probably NOT])


class BaseRateLimiter(abc.ABC):
    """
    This is the interface that must be implemented to satisfy wiji's rate limiting.
    User implementations should inherit this class and
    implement the :func:`limit <BaseRateLimiter.limit>` and :func:`notify <BaseRateLimiter.notify>` methods with the type signatures shown.

    It may be important to control the rate at which the worker(wiji) consumes/executes tasks.
    wiji lets you do this, by allowing you to specify a custom rate limiter.
    """

    @abc.abstractmethod
    async def limit(self) -> None:
        """
        rate limit consumation/execution of tasks.
        """
        raise NotImplementedError("`limit` method must be implemented.")

    @abc.abstractmethod
    async def notify(
        self,
        task_name: str,
        task_id: str,
        queue_name: str,
        state: "task.TaskState",
        queuing_duration: typing.Union[None, typing.Dict[str, float]] = None,
        queuing_exception: typing.Union[None, Exception] = None,
        execution_duration: typing.Union[None, typing.Dict[str, float]] = None,
        execution_exception: typing.Union[None, Exception] = None,
        return_value: typing.Union[None, typing.Any] = None,
    ) -> None:
        """
        this method is called:
          - by `wiji.task.Task` once it has finished queuing a task to the broker
          - by `wiji.worker.worker` once it has finished executing a task.
        implementers may choose to use the metrics provided to dynamically adjust their rate limiting policies.
        """
        raise NotImplementedError("`notify` method must be implemented.")


class SimpleRateLimiter(BaseRateLimiter):
    """
    This is an implementation of BaseRateLimiter.

    It does rate limiting using a `token bucket rate limiting algorithm <https://en.wikipedia.org/wiki/Token_bucket>`_

    example usage:

    .. code-block:: python

        rateLimiter = SimpleRateLimiter(log_handler=myLogger, execution_rate=10.0)
        await rateLimiter.limit()
    """

    def __init__(
        self,
        execution_rate: float = 100_000_000.0,
        log_handler: typing.Union[None, logger.BaseLogger] = None,
    ) -> None:
        """
        Parameters:
            execution_rate: the maximum rate, in tasks/second, at which wiji can consume/execute tasks.
        """
        self._validate_args(execution_rate=execution_rate, log_handler=log_handler)

        self.execution_rate: float = execution_rate
        self.max_tokens: float = self.execution_rate
        self.tokens: float = self.max_tokens

        self.delay_for_tokens: float = 1.0
        self.updated_at: float = time.monotonic()
        self.tasks_executed: int = 0
        self.task_processing_rate: float = self.execution_rate

        if log_handler is not None:
            self.logger = log_handler
        else:
            self.logger = logger.SimpleLogger("wiji.ratelimiter.SimpleRateLimiter")

    def _validate_args(self, execution_rate, log_handler):
        if not isinstance(execution_rate, float):
            raise ValueError(
                """`execution_rate` should be of type:: `float` You entered: {0}""".format(
                    type(execution_rate)
                )
            )
        if execution_rate < 1.0:
            raise ValueError(
                """`execution_rate` should not be less than 1.0 You entered: {0}""".format(
                    execution_rate
                )
            )
        if not isinstance(log_handler, (type(None), logger.BaseLogger)):
            raise ValueError(
                """`log_handler` should be of type:: `None` or `wiji.logger.BaseLogger` You entered: {0}""".format(
                    type(log_handler)
                )
            )

    def _add_new_tokens(self) -> None:
        now = time.monotonic()
        time_since_update = now - self.updated_at
        self.task_processing_rate = self.tasks_executed / time_since_update
        new_tokens = time_since_update * self.execution_rate
        if new_tokens > 1:
            self.tokens = min(self.tokens + new_tokens, self.max_tokens)
            self.updated_at = now
            self.tasks_executed = 0

    async def limit(self) -> None:
        self.logger.log(logging.INFO, {"event": "wiji.SimpleRateLimiter.limit", "stage": "start"})
        while self.tokens < 1:
            self._add_new_tokens()
            # todo: sleep in an exponetial manner upto a maximum then wrap around.
            await asyncio.sleep(self.delay_for_tokens)
            self.logger.log(
                logging.INFO,
                {
                    "event": "wiji.SimpleRateLimiter.limit",
                    "stage": "end",
                    "state": "limiting rate",
                    "execution_rate": self.execution_rate,
                    "delay": self.delay_for_tokens,
                    "task_processing_rate": self.task_processing_rate,
                },
            )
        self.tasks_executed += 1
        self.tokens -= 1
        self.logger.log(
            logging.INFO,
            {
                "event": "wiji.SimpleRateLimiter.limit",
                "stage": "end",
                "task_processing_rate": self.task_processing_rate,
            },
        )

    async def notify(
        self,
        task_name: str,
        task_id: str,
        queue_name: str,
        state: "task.TaskState",
        queuing_duration: typing.Union[None, typing.Dict[str, float]] = None,
        queuing_exception: typing.Union[None, Exception] = None,
        execution_duration: typing.Union[None, typing.Dict[str, float]] = None,
        execution_exception: typing.Union[None, Exception] = None,
        return_value: typing.Union[None, typing.Any] = None,
    ) -> None:
        """
        SimpleRateLimiter does nothing with the data/metrics it gets about queuing and execution outcomes.
        However, you can imagine a smarter RateLimiter that uses these metrics to dynamically change
        its rate-limiting methodologies, eg:
          - increase ratelimit if percentage of exceptions goes up. or
          - increase ratelimit if queuing_duration is very high.
        """
        if queue_name != task._watchdogTask.queue_name:
            self.logger.log(
                logging.DEBUG,
                {
                    "event": "wiji.SimpleRateLimiter.notify",
                    "stage": "end",
                    "state": state,
                    "task_name": task_name,
                    "task_id": task_id,
                    "queue_name": queue_name,
                    "queuing_duration": queuing_duration,
                    "queuing_exception": str(queuing_exception),
                    "execution_duration": execution_duration,
                    "execution_exception": str(execution_exception),
                    "return_value": return_value,
                },
            )
