import abc
import typing
import asyncio

if typing.TYPE_CHECKING:
    from . import task


class BaseBroker(abc.ABC):
    """
    This is the interface that must be implemented to satisfy wiji's broker.
    User implementations should inherit this class and
    implement the :func:`check <BaseBroker.check>`, :func:`enqueue <BaseBroker.enqueue>` and
    :func:`dequeue <BaseBroker.dequeue>` methods with the type signatures shown.

    wiji calls an implementation of this class to enqueue and/or dequeue an item.
    """

    @abc.abstractmethod
    async def check(self, queue_name: str) -> None:
        """
        called by `wiji` worker once, during startup so as;
          - to check that the broker is up
          - to inform the broker of the queue_name that the worker will be consuming from.
            the broker can go ahead and create this queue_name if it does not exist
        """
        raise NotImplementedError("`check` method must be implemented.")

    @abc.abstractmethod
    async def enqueue(self, queue_name: str, item: str) -> None:
        """
        enqueue/save an item.

        Parameters:
            item: The item to be enqueued/saved
                  that item looks like:
                        {
                            "version": 1,
                            "task_options": {
                                "eta": "ISO 8601-formatted datetime",
                                "task_id": "some-uuid4",
                                "current_retries": 0,
                                "max_retries": 0,
                                "hook_metadata": "",
                                "args": [],
                                "kwargs": {},
                            },
                        }
            queue_name: name of queue to enqueue in
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

    @abc.abstractmethod
    async def done(self, queue_name: str, item: str, state: "task.TaskState") -> None:
        """
        called by wiji worker once it is done executing a task.
        the broker can then decide to do any clean up actions like removing that task from the queue etc.
        """
        raise NotImplementedError("`done` method must be implemented.")

    @abc.abstractmethod
    async def shutdown(self, queue_name: str, duration: float) -> None:
        """
        called by wiji worker when it receives a shutdown signal like `SIGTERM`.
        the broker can decide to perform shutdown procedures like releasing connections/file descriptors etc.

        Parameters:
            queue_name: name of queue which the wiji worker was consuming from
            duration: duration in seconds that wiji worker will wait for after calling this method.
        """
        raise NotImplementedError("`shutdown` method must be implemented.")


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
        # max tasks we can store per queue_name.
        self.max_tasks: int = 2000

    async def check(self, queue_name: str) -> None:
        if queue_name not in self.store:
            self.store[queue_name] = []
        await asyncio.sleep(0.00000000001)

    async def enqueue(self, queue_name: str, item: str) -> None:
        if self.store.get(queue_name):
            self.store[queue_name].append(item)
            # NB: without this awaits, only tasks scheduled in the InMemoryBroker(like `WatchDogTask`)
            # would get priority since they wouldn't be co-operative in their scheduling
            await asyncio.sleep(delay=-1)
        else:
            self.store[queue_name] = [item]
            await asyncio.sleep(delay=-1)

        await self._guard_leak(queue_name=queue_name)

    async def dequeue(self, queue_name: str) -> str:
        while True:
            if queue_name in self.store:
                try:
                    return await asyncio.sleep(delay=-1, result=self.store[queue_name].pop(0))
                except IndexError:
                    # queue is empty
                    await asyncio.sleep(0.25)
            else:
                raise ValueError("queue with name: {0} does not exist.".format(queue_name))

    async def done(self, queue_name: str, item: str, state: "task.TaskState") -> None:
        """
        for this broker, this method is not needed, since `dequeue` uses .pop() which deletes the item.
        """
        if queue_name in self.store:
            return await asyncio.sleep(delay=-1, result=None)
        else:
            raise ValueError("queue with name: {0} does not exist.".format(queue_name))

    async def shutdown(self, queue_name: str, duration: float) -> None:
        return await asyncio.sleep(delay=-1, result=None)

    def _llen(self, queue_name: str) -> int:
        """
        find the length/size/number of queued items in the given queue.
        Only used in tests.
        """
        return len(self.store[queue_name])

    async def _guard_leak(self, queue_name: str) -> None:
        """
        This func guards against the possibility of the `InMemoryBroker` leaking memory.
        Since this broker uses a dict as its backing storage, if the number of items appended
        to that dict keeps on rising over time without reducing; a memory leak may occur.

        So this func makes sure that the number of items in `self.store` for a given `queue_name`
        never goes above X items. If it does, this func will clear that queue back to empty.
        Since this broker is only used for demo purposes and by `wiji.task.WatchDogTask`
        it's okay to empty queues in an ad hoc manner.

        This functionality was added to fix an actual memory leak.
        see: https://github.com/komuw/wiji/issues/71
        """
        _llen = self._llen(queue_name=queue_name)
        if _llen > self.max_tasks:
            self.store[queue_name] = []
