import asyncio
import random
import string
import logging

from . import logger


class Worker:
    """
    """

    def __init__(
        self,
        async_loop: asyncio.events.AbstractEventLoop,
        client_id=None,
        log_handler=None,
        loglevel: str = "DEBUG",
        log_metadata=None,
    ) -> None:
        """
        """
        if loglevel.upper() not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(
                """loglevel should be one of; 'DEBUG', 'INFO', 'WARNING', 'ERROR' or 'CRITICAL'. not {0}""".format(
                    loglevel
                )
            )
        if not isinstance(log_metadata, (type(None), dict)):
            raise ValueError(
                """log_metadata should be of type:: None or dict. You entered {0}""".format(
                    type(log_metadata)
                )
            )

        self.async_loop = async_loop
        self.loglevel = loglevel.upper()

        self.client_id = client_id
        if not self.client_id:
            self.client_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))

        self.log_metadata = log_metadata
        if not self.log_metadata:
            self.log_metadata = {}
        self.log_metadata.update({"client_id": self.client_id})

        self.logger = log_handler
        if not self.logger:
            self.logger = logger.SimpleBaseLogger("naz.client")
        self.logger.bind(loglevel=self.loglevel, log_metadata=self.log_metadata)
        self._sanity_check_logger()

    def _sanity_check_logger(self):
        """
        called when instantiating the Worker just to make sure the supplied
        logger can log.
        """
        try:
            self.logger.log(logging.DEBUG, {"event": "sanity_check_logger"})
        except Exception as e:
            raise e

    def _log(self, level, log_data):
        # if the supplied logger is unable to log; we move on
        try:
            self.logger.log(level, log_data)
        except Exception:
            pass

    async def run(self):
        # this is where people put their code.
        pass
