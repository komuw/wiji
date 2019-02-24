import abc
import time
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
            await asyncio.sleep(delay=-1)
        else:
            self.store[queue_name] = [item]
            await asyncio.sleep(delay=-1)

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


class YoloBroker(BaseBroker):
    """
    This is an in-memory implementation of BaseBroker.

    Note: It should only be used for tests and demo purposes.
    """

    def __init__(self, maxsize: int = 0) -> None:
        """
        Parameters:
            maxsize: the maximum number of items(not size) that can be put in the queue.
            loop: an event loop
        """
        self.queue: asyncio.queues.Queue = asyncio.Queue(maxsize=maxsize)
        self.store: dict = {}
        self.max_ttl: float = 20 * 60  # 20mins
        self.start_timer = time.monotonic()

    async def enqueue(self, item: str, queue_name: str) -> None:
        if self.store.get(queue_name):
            self.store[queue_name].append(item)
        else:
            self.store[queue_name] = [item]
        self.queue.put_nowait(self.store)

        # garbage collect
        await self.delete_after_ttl()

    async def dequeue(self, queue_name: str) -> str:
        store = await self.queue.get()
        if queue_name in store:
            try:
                return self.store[queue_name].pop(0)
            except IndexError:
                # queue is empty
                await asyncio.sleep(1.5)
        else:
            raise ValueError("queue with name: {0} does not exist.".format(queue_name))

    async def delete_after_ttl(self) -> None:
        """
        iterate over all stored items and delete any that are
        older than self.max_ttl seconds
        """
        now = time.monotonic()
        time_diff = now - self.start_timer
        if time_diff > self.max_ttl:
            for key in list(self.store.keys()):
                self.store[key] = []
