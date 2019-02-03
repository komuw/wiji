import abc
import asyncio
import typing


class BaseQueue(abc.ABC):
    """
    This is the interface that must be implemented to satisfy xyzabc's outbound queue.
    User implementations should inherit this class and
    implement the :func:`enqueue <BaseQueue.enqueue>` and :func:`dequeue <BaseQueue.dequeue>` methods with the type signatures shown.

    xyzabc calls an implementation of this class to enqueue and/or dequeue an item.
    """

    @abc.abstractmethod
    async def enqueue(self, item: dict) -> None:
        """
        enqueue/save an item.

        Parameters:
            item: The item to be enqueued/saved
        """
        raise NotImplementedError("enqueue method must be implemented.")

    @abc.abstractmethod
    async def dequeue(self) -> typing.Dict[typing.Any, typing.Any]:
        """
        dequeue an item.

        Returns:
            item that was dequeued
        """
        raise NotImplementedError("dequeue method must be implemented.")


class SimpleOutboundQueue(BaseQueue):
    """
    This is an in-memory implementation of BaseQueue.

    Note: It should only be used for tests and demo purposes.
    """

    def __init__(self, maxsize: int, loop: asyncio.events.AbstractEventLoop) -> None:
        """
        Parameters:
            maxsize: the maximum number of items(not size) that can be put in the queue.
            loop: an event loop
        """
        self.queue: asyncio.queues.Queue = asyncio.Queue(maxsize=maxsize, loop=loop)

    async def enqueue(self, item: dict) -> None:
        self.queue.put_nowait(item)

    async def dequeue(self) -> typing.Dict[typing.Any, typing.Any]:
        return await self.queue.get()
