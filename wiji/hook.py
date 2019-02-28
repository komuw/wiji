import abc
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
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
    When this class is called by wiji, it just logs the request or response.
    """

    def __init__(self, logger) -> None:
        self.logger: logging.Logger = logger

    async def request(self, task_id: str, log_id: str, hook_metadata: str) -> None:
        self.logger.log(
            logging.INFO,
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
            logging.INFO,
            {
                "event": "wiji.SimpleHook.response",
                "stage": "start",
                "task_id": task_id,
                "log_id": log_id,
                "hook_metadata": hook_metadata,
            },
        )
