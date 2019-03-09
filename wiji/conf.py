import typing

from . import task


class WijiConf:
    def __init__(self, tasks: typing.List["task.Task"], watchdog_duration: float = 0.1) -> None:
        self._validate_config_args(tasks=tasks, watchdog_duration=watchdog_duration)

        self.tasks = tasks
        self.watchdog_duration = watchdog_duration

    def _validate_config_args(self, tasks, watchdog_duration):
        if not isinstance(tasks, list):
            raise ValueError(
                """`tasks` should be of type:: `list` You entered: {0}""".format(type(tasks))
            )
        for tsk in tasks:
            if not isinstance(tsk, task.Task):
                raise ValueError(
                    """`task` should be of type:: `wiji.task.Task` You entered: {0}""".format(
                        type(tsk)
                    )
                )
        if not isinstance(watchdog_duration, float):
            raise ValueError(
                """`watchdog_duration` should be of type:: `float` You entered: {0}""".format(
                    type(watchdog_duration)
                )
            )
