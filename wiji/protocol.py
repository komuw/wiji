import json
import typing
import datetime


class Protocol:
    def __init__(
        self,
        version: int,
        task_id: str,
        eta: typing.Union[float, str],
        current_retries: int,
        max_retries: int,
        log_id: str,
        hook_metadata: str,
        argsy: tuple,
        kwargsy: dict,
    ):
        self._validate_protocol_args(
            version=version,
            task_id=task_id,
            eta=eta,
            current_retries=current_retries,
            max_retries=max_retries,
            log_id=log_id,
            hook_metadata=hook_metadata,
            argsy=argsy,
            kwargsy=kwargsy,
        )

        self.version = version
        self.task_id = task_id
        self.current_retries = current_retries
        self.max_retries = max_retries
        self.log_id = log_id
        self.hook_metadata = hook_metadata
        self.argsy = argsy
        self.kwargsy = kwargsy

        if isinstance(eta, float):
            self.eta = self._eta_to_isoformat(eta=eta)
        else:
            self.eta = eta

    def _validate_protocol_args(
        self,
        version,
        task_id,
        eta,
        current_retries,
        max_retries,
        log_id,
        hook_metadata,
        argsy,
        kwargsy,
    ):
        if not isinstance(version, int):
            raise ValueError(
                """`version` should be of type:: `int` You entered: {0}""".format(type(version))
            )
        if not isinstance(task_id, str):
            raise ValueError(
                """`task_id` should be of type:: `str` You entered: {0}""".format(type(task_id))
            )
        if not isinstance(eta, (str, float)):
            raise ValueError(
                """`eta` should be of type:: `str` or `float` You entered: {0}""".format(type(eta))
            )
        if isinstance(eta, str):
            try:
                # type-check that string eta is ISO 8601-formatted
                self._from_isoformat(eta)
            except Exception as e:
                raise ValueError(
                    """`eta` in string format should be a python ISO 8601-formatted string. You entered: {0}""".format(
                        eta
                    )
                ) from e
        if not isinstance(current_retries, int):
            raise ValueError(
                """`current_retries` should be of type:: `int` You entered: {0}""".format(
                    type(current_retries)
                )
            )
        if not isinstance(max_retries, int):
            raise ValueError(
                """`max_retries` should be of type:: `int` You entered: {0}""".format(
                    type(max_retries)
                )
            )
        if not isinstance(log_id, str):
            raise ValueError(
                """`log_id` should be of type:: `str` You entered: {0}""".format(type(log_id))
            )
        if not isinstance(hook_metadata, (type(None), str)):
            raise ValueError(
                """`hook_metadata` should be of type:: `None` or `str` You entered: {0}""".format(
                    type(hook_metadata)
                )
            )
        if not isinstance(argsy, tuple):
            raise ValueError(
                """`argsy` should be of type:: `tuple` You entered: {0}""".format(type(argsy))
            )
        if not isinstance(kwargsy, dict):
            raise ValueError(
                """`kwargsy` should be of type:: `dict` You entered: {0}""".format(type(kwargsy))
            )

    @staticmethod
    def _eta_to_isoformat(eta):
        """
        converts eta in float seconds to python's ISO 8601-formatted datetime.
        """
        eta = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=eta)
        eta = eta.isoformat()
        return eta

    @staticmethod
    def _from_isoformat(eta):
        """
        converts ISO 8601-formatted datetime to python datetime
        usage:
            dt = Protocol._from_isoformat(eta="2020-04-01T18:41:54.195638+00:00")
        """
        dt = datetime.datetime.strptime(eta, "%Y-%m-%dT%H:%M:%S.%f%z")
        return dt

    def json(self):
        return json.dumps(
            {
                "version": self.version,
                "task_id": self.task_id,
                "eta": self.eta,
                "current_retries": self.current_retries,
                "max_retries": self.max_retries,
                "log_id": self.log_id,
                "hook_metadata": self.hook_metadata,
                "args": self.argsy,
                "kwargs": self.kwargsy,
            }
        )
