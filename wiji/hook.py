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
        queuing_duration: typing.Union[None, typing.Dict[str, float]] = None,
        queuing_exception: typing.Union[None, Exception] = None,
        execution_duration: typing.Union[None, typing.Dict[str, float]] = None,
        execution_exception: typing.Union[None, Exception] = None,
        return_value: typing.Union[None, typing.Any] = None,
    ) -> None:
        """
        called by `wiji` worker whenever a task undergoes a state change.
        """
        raise NotImplementedError("`notify` method must be implemented.")


class SimpleHook(BaseHook):
    """
    This is an implementation of BaseHook.
    It implements a no-op hook that does nothing when called
    """

    def __init__(self, log_handler: typing.Union[None, logger.BaseLogger] = None) -> None:
        if log_handler is not None:
            self.logger = log_handler
        else:
            self.logger = logger.SimpleLogger("wiji.hook.SimpleHook")

    async def notify(
        self,
        task_name: str,
        task_id: str,
        queue_name: str,
        hook_metadata: str,
        state: "task.TaskState",
        queuing_duration: typing.Union[None, typing.Dict[str, float]] = None,
        queuing_exception: typing.Union[None, Exception] = None,
        execution_duration: typing.Union[None, typing.Dict[str, float]] = None,
        execution_exception: typing.Union[None, Exception] = None,
        return_value: typing.Union[None, typing.Any] = None,
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
                "queuing_duration": queuing_duration,
                "queuing_exception": queuing_exception,
                "execution_duration": execution_duration,
                "execution_exception": str(execution_exception),
                "return_value": str(return_value),
            },
        )
