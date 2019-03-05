import abc
import time
import asyncio
import typing
import json

from . import task
from . import protocol


class BaseBroker(abc.ABC):
    """
    This is the interface that must be implemented to satisfy wiji's broker.
    User implementations should inherit this class and
    implement the :func:`enqueue <BaseBroker.enqueue>` and :func:`dequeue <BaseBroker.dequeue>` methods with the type signatures shown.

    wiji calls an implementation of this class to enqueue and/or dequeue an item.
    """

    @abc.abstractmethod
    async def enqueue(self, item: str, queue_name: str, task_options: "task.TaskOptions") -> None:
        """
        enqueue/save an item.

        Parameters:
            item: The item to be enqueued/saved
            queue_name: name of queue to enqueue in
            task_options: options for the specific task been enqueued
        """
        raise NotImplementedError("`enqueue` method must be implemented.")

    @abc.abstractmethod
    async def dequeue(self, queue_name: str) -> str:
        """
        dequeue an item.

        Returns:
            item that was dequeued
        """
        raise NotImplementedError("`dequeue` method must be implemented.")


class InMemoryBroker(BaseBroker):
    """
    This broker should only be used for:
      (i) tests
      (ii) demos
      (iii) the watchdog task.
    Do not use this broker in production or anywhere else that you care about.

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
        self._create_watchdog_queue()

    async def enqueue(self, item: str, queue_name: str, task_options: "task.TaskOptions") -> None:
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

    def _create_watchdog_queue(self):
        self.store[task._watchdogTask.queue_name] = []
