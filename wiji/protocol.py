import json
import typing
import datetime

if typing.TYPE_CHECKING:
    from . import task


class Protocol:
    def __init__(self, version: int, task_options: "task.TaskOptions") -> None:
        self._validate_protocol_args(version=version, task_options=task_options)
        self.version = version
        self.task_options = task_options

    def _validate_protocol_args(self, version: int, task_options: "task.TaskOptions"):
        if not isinstance(version, int):
            raise ValueError(
                """`version` should be of type:: `int` You entered: {0}""".format(type(version))
            )

        from . import task

        if not isinstance(task_options, task.TaskOptions):
            raise ValueError(
                """`task_options` should be of type:: `wiji.task.TaskOptions` You entered: {0}""".format(
                    type(task_options)
                )
            )

        if not isinstance(task_options.eta, str):
            raise ValueError(
                """`task.TaskOptions.eta` should be of type:: `str` You entered: {0}""".format(
                    type(task_options.eta)
                )
            )
        try:
            # type-check that string eta is ISO 8601-formatted
            self._from_isoformat(task_options.eta)
        except Exception as e:
            raise ValueError(
                """`task.TaskOptions.eta` in string format should be a python ISO 8601-formatted string. You entered: {0}""".format(
                    task_options.eta
                )
            ) from e

        if not isinstance(task_options.current_retries, int):
            raise ValueError(
                """`task.TaskOptions.current_retries` should be of type:: `int` You entered: {0}""".format(
                    type(task_options.current_retries)
                )
            )
        if not isinstance(task_options.max_retries, int):
            raise ValueError(
                """`task.TaskOptions.max_retries` should be of type:: `int` You entered: {0}""".format(
                    type(task_options.max_retries)
                )
            )
        if not isinstance(task_options.hook_metadata, (type(None), str)):
            raise ValueError(
                """`task.TaskOptions.hook_metadata` should be of type:: `None` or `str` You entered: {0}""".format(
                    type(task_options.hook_metadata)
                )
            )
        if not isinstance(task_options.args, tuple):
            raise ValueError(
                """`task.TaskOptions.args` should be of type:: `tuple` You entered: {0}""".format(
                    type(task_options.args)
                )
            )
        if not isinstance(task_options.kwargs, dict):
            raise ValueError(
                """`task.TaskOptions.kwargs` should be of type:: `dict` You entered: {0}""".format(
                    type(task_options.kwargs)
                )
            )

    @staticmethod
    def _eta_to_isoformat(eta) -> str:
        """
        converts eta in float seconds to python's ISO 8601-formatted datetime.
        """
        eta = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=eta)
        eta = eta.isoformat()
        return eta

    @staticmethod
    def _from_isoformat(eta) -> datetime.datetime:
        """
        converts ISO 8601-formatted datetime to python datetime
        usage:
            dt = Protocol._from_isoformat(eta="2020-04-01T18:41:54.195638+00:00")
        """
        dt = datetime.datetime.fromisoformat(eta)
        return dt

    def json(self) -> str:
        return json.dumps({"version": self.version, "task_options": self.task_options.dictsy()})
