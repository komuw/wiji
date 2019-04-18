import typing
import inspect

from . import task


class App:
    def __init__(
        self, task_classes: typing.List[typing.Type[task.Task]], watchdog_duration: float = 0.1
    ) -> None:
        self._validate_app_args(task_classes=task_classes, watchdog_duration=watchdog_duration)

        self.task_classes = task_classes
        self.watchdog_duration = watchdog_duration

    @staticmethod
    def _validate_app_args(
        task_classes: typing.List[typing.Type[task.Task]], watchdog_duration: float
    ):
        if not isinstance(task_classes, list):
            raise ValueError(
                """`task_classes` should be of type:: `list` You entered: {0}""".format(
                    type(task_classes)
                )
            )
        for tsk in task_classes:
            if not inspect.isclass(tsk):
                raise ValueError(
                    """each element of `task_classes` should be a class and NOT a class instance"""
                )
            if not issubclass(tsk, task.Task):
                raise ValueError(
                    """each element of `task_classes` should be a subclass of:: `wiji.task.Task`"""
                )

        if not isinstance(watchdog_duration, float):
            raise ValueError(
                """`watchdog_duration` should be of type:: `float` You entered: {0}""".format(
                    type(watchdog_duration)
                )
            )
