import os
import asyncio
import functools
import concurrent


import wiji
import redis


class ExampleRedisBroker(wiji.broker.BaseBroker):
    """
    DO NOT USE THIS IN PRODUCTION.
    It will fail you, this is only used for adhoc testing
    and for showing how easy it is to implement your own broker satisfying `wiji.broker.BaseBroker`

    use redis as our queue.
    This implements a basic FIFO queue using redis.
    Basically we use the redis command LPUSH to push messages onto the queue and BRPOP to pull them off.
    https://redis.io/commands/lpush
    https://redis.io/commands/brpop
    Note that in practice, you would probaly want to use a non-blocking redis
    client eg https://github.com/aio-libs/aioredis
    This example uses concurrent.futures.ThreadPoolExecutor to workaround
    the fact that we are using a blocking/sync redis client.
    Use an async client in real life/code.
    """

    def __init__(self):
        host = "localhost"
        port = 6379
        password = None
        if os.environ.get("IN_DOCKER"):
            host = os.environ["REDIS_HOST"]
            port = os.environ["REDIS_PORT"]
            password = os.environ.get("REDIS_PASSWORD", None)
        port = int(port)
        self.redis_instance = redis.StrictRedis(
            host=host, port=port, password=password, db=0, socket_timeout=8.0
        )

    async def check(self, queue_name: str) -> None:
        await asyncio.sleep(1 / 117)

    async def enqueue(self, item: str, queue_name: str) -> None:
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix="wiji-redis-thread-pool"
        ) as executor:
            await self.loop.run_in_executor(
                executor,
                functools.partial(self._blocking_enqueue, queue_name=queue_name, item=item),
            )

    def _blocking_enqueue(self, queue_name, item):
        self.redis_instance.lpush(queue_name, item)

    async def dequeue(self, queue_name: str) -> str:
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix="wiji-redis-thread-pool"
        ) as executor:
            while True:
                item = await self.loop.run_in_executor(
                    executor, functools.partial(self._blocking_dequeue, queue_name=queue_name)
                )
                if item:
                    return item
                else:
                    await asyncio.sleep(1 / 117)

    def _blocking_dequeue(self, queue_name: str):
        dequed_item = self.redis_instance.brpop(queue_name, timeout=3)
        if not dequed_item:
            return None
        dequed_item = dequed_item[1]
        return dequed_item

    async def done(self, item: str, queue_name: str, state: wiji.task.TaskState) -> None:
        # dequeue already removed the item
        return await asyncio.sleep(delay=0.02, result=None)

    async def shutdown(self, queue_name: str, duration: float) -> None:
        return await asyncio.sleep(delay=-1, result=None)

    def _flushdb(self):
        """
        delete all keys in the current database.
        Only used in tests to ensure each testcase starts off with a fresh DB
        """
        self.redis_instance.flushdb()

    def _llen(self, queue_name: str):
        """
        find the length/size/number of queued items in the given queue.
        Only used in tests.
        """
        return self.redis_instance.llen(queue_name)
