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
    async def enqueue(self, item: str, queue_name: str) -> None:
        """
        enqueue/save an item.

        Parameters:
            item: The item to be enqueued/saved
            queue_name: name of queue to enqueue in
        """
        raise NotImplementedError("enqueue method must be implemented.")

    @abc.abstractmethod
    async def dequeue(self, queue_name: str) -> str:
        """
        dequeue an item.

        Returns:
            item that was dequeued
        """
        raise NotImplementedError("dequeue method must be implemented.")


class SimpleOutboundQueue(BaseQueue):
    """
    {
        "queue1": ["item1", "item2", "item3"],
        "queue2": ["item1", "item2", "item3"]
        ...
    }
    """

    def __init__(self) -> None:
        """
        """
        self.store: dict = {}

    async def enqueue(self, item: str, queue_name: str) -> None:
        if self.store.get(queue_name):
            self.store[queue_name].append(item)
        else:
            self.store[queue_name] = [item]

    async def dequeue(self, queue_name: str) -> str:
        if self.store.get(queue_name):
            try:
                return await asyncio.sleep(delay=-1, result=self.store[queue_name].pop(0))
            except IndexError:
                return await asyncio.sleep(delay=-1, result=None)
        else:
            return await asyncio.sleep(delay=-1, result=None)
