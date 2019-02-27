import json


class Protocol:
    def __init__(
        self,
        version: int,
        task_id: str,
        eta: float,
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
        self.eta = eta
        self.current_retries = current_retries
        self.max_retries = max_retries
        self.log_id = log_id
        self.hook_metadata = hook_metadata
        self.argsy = argsy
        self.kwargsy = kwargsy

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
        if not isinstance(eta, float):
            raise ValueError(
                """`eta` should be of type:: `float` You entered: {0}""".format(type(eta))
            )

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
