import abc
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import xyzabc  # noqa: F401


class BaseHook(abc.ABC):
    """
    """

    @abc.abstractmethod
    async def request(self, log_id: str) -> None:
        """
        """
        raise NotImplementedError("request method must be implemented.")

    @abc.abstractmethod
    async def response(self, log_id: str) -> None:
        """
        """
        raise NotImplementedError("response method must be implemented.")
