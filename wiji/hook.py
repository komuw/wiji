import abc
import typing
import logging

from . import logger

if typing.TYPE_CHECKING:
    import wiji  # noqa: F401


class BaseHook(abc.ABC):
    """
    """

    @abc.abstractmethod
    async def request(self, task_id: str, log_id: str, hook_metadata: str) -> None:
        """
        """
        raise NotImplementedError("`request` method must be implemented.")

    @abc.abstractmethod
    async def response(self, task_id: str, log_id: str, hook_metadata: str) -> None:
        """
        """
        raise NotImplementedError("`response` method must be implemented.")


class SimpleHook(BaseHook):
    """
    This is an implementation of BaseHook.
    It implements a no-op hook that does nothing when called
    """

    def __init__(self, logger: typing.Union[None, logging.LoggerAdapter] = None) -> None:
        self.logger = logger
        if not self.logger:
            self.logger = logger.SimpleLogger("wiji.hook.SimpleHook")

    async def request(self, task_id: str, log_id: str, hook_metadata: str) -> None:
        self.logger.log(
            logging.NOTSET,
            {
                "event": "wiji.SimpleHook.request",
                "stage": "start",
                "task_id": task_id,
                "log_id": log_id,
                "hook_metadata": hook_metadata,
            },
        )

    async def response(self, task_id: str, log_id: str, hook_metadata: str) -> None:
        self.logger.log(
            logging.NOTSET,
            {
                "event": "wiji.SimpleHook.response",
                "stage": "start",
                "task_id": task_id,
                "log_id": log_id,
                "hook_metadata": hook_metadata,
            },
        )
