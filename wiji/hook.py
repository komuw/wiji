import abc
import typing
import logging

from . import logger

if typing.TYPE_CHECKING:
    from . import task


class BaseHook(abc.ABC):
    """
    """

    @abc.abstractmethod
    async def notify(
        self,
        task_name: str,
        task_id: str,
        queue_name: str,
        hook_metadata: str,
        state: "task.TaskState",
        execution_duration: typing.Dict[str, float],
    ) -> None:
        """
        """
        raise NotImplementedError("`notify` method must be implemented.")


class SimpleHook(BaseHook):
    """
    This is an implementation of BaseHook.
    It implements a no-op hook that does nothing when called
    """

    def __init__(self, log_handler: typing.Union[None, logging.LoggerAdapter] = None) -> None:
        self.logger = log_handler
        if not self.logger:
            self.logger = logger.SimpleLogger("wiji.hook.SimpleHook")

    async def notify(
        self,
        task_name: str,
        task_id: str,
        queue_name: str,
        hook_metadata: str,
        state: "task.TaskState",
        execution_duration: typing.Dict[str, float],
    ) -> None:
        self.logger.log(
            logging.NOTSET,
            {
                "event": "wiji.SimpleHook.notify",
                "stage": "start",
                "state": state,
                "task_name": task_name,
                "task_id": task_id,
                "queue_name": queue_name,
                "hook_metadata": hook_metadata,
                "execution_duration": execution_duration,
            },
        )
