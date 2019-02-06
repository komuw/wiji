import abc
import asyncio
import typing


class BaseBroker(abc.ABC):
    """
    This is the interface that must be implemented to satisfy xyzabc's broker.
    User implementations should inherit this class and
    implement the :func:`enqueue <BaseBroker.enqueue>` and :func:`dequeue <BaseBroker.dequeue>` methods with the type signatures shown.

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


class SimpleBroker(BaseBroker):
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
        while True:
            if queue_name in self.store:
                try:
                    return await asyncio.sleep(delay=-1, result=self.store[queue_name].pop(0))
                except IndexError:
                    # queue is empty
                    await asyncio.sleep(5)
            else:
                raise ValueError("queue with name: {0} does not exist.".format(queue_name))
