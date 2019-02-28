import abc
import time
import asyncio
import logging


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
    implement the :func:`limit <BaseRateLimiter.limit>` methods with the type signatures shown.

    It may be important to control the rate at which the worker(wiji) consumes/executes tasks.
    wiji lets you do this, by allowing you to specify a custom rate limiter.
    """

    @abc.abstractmethod
    async def limit(self) -> None:
        """
        rate limit consumation/execution of tasks.
        """
        raise NotImplementedError("`limit` method must be implemented.")


class SimpleRateLimiter(BaseRateLimiter):
    """
    This is an implementation of BaseRateLimiter.

    It does rate limiting using a `token bucket rate limiting algorithm <https://en.wikipedia.org/wiki/Token_bucket>`_

    example usage:

    .. code-block:: python

        rateLimiter = SimpleRateLimiter(logger=myLogger, execution_rate=10, max_tokens=25)
        await rateLimiter.limit()
    """

    def __init__(
        self,
        logger: logging.LoggerAdapter,
        execution_rate: float = 100_000_000,
        max_tokens: float = 100_000_000,
        delay_for_tokens: float = 1,
    ) -> None:
        """
        Parameters:
            execution_rate: the maximum rate, in tasks/second, at which wiji can consume/execute tasks.
            max_tokens: the total number of tasks wiji can consume/execute before rate limiting kicks in.
            delay_for_tokens: the duration in seconds which to wait for before checking for token availability after they had finished.

        execution_rate and max_tokens should generally be of equal value.
        """
        self.execution_rate: float = execution_rate
        self.max_tokens: float = max_tokens
        self.delay_for_tokens: float = (delay_for_tokens)
        self.tokens: float = self.max_tokens
        self.updated_at: float = time.monotonic()

        self.logger = logger
        self.tasks_executed: int = 0
        self.effective_execution_rate: float = 0

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
                    "effective_execution_rate": self.effective_execution_rate,
                },
            )

        self.tasks_executed += 1
        self.tokens -= 1

    def _add_new_tokens(self) -> None:
        now = time.monotonic()
        time_since_update = now - self.updated_at
        self.effective_execution_rate = self.tasks_executed / time_since_update
        new_tokens = time_since_update * self.execution_rate
        if new_tokens > 1:
            self.tokens = min(self.tokens + new_tokens, self.max_tokens)
            self.updated_at = now
            self.tasks_executed = 0
