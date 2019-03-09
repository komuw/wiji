import asyncio
import functools
import concurrent


import wiji
import redis


def BLOCKING_DISK_IO(the_broker) -> wiji.task.Task:
    class BlockingDiskIOTask(wiji.task.Task):
        async def run(self, *args, **kwargs):
            print()
            print("RUNNING BlockingDiskIOTask:")
            import subprocess

            subprocess.run(["dd", "if=/dev/zero", "of=/dev/null", "bs=500000", "count=1000000"])

    task = BlockingDiskIOTask(the_broker=the_broker, queue_name="BlockingDiskIOTask")
    return task


def BLOCKING_http_task(the_broker) -> wiji.task.Task:
    class BlockinTask(wiji.task.Task):
        async def run(self, *args, **kwargs):
            print()
            print("RUNNING BLOCKING_http_task:")
            import requests

            url = kwargs["url"]
            resp = requests.get(url)
            print("resp: ", resp)

    task = BlockinTask(the_broker=the_broker, queue_name="BlockingHttp_Queue")
    return task


def http_task(the_broker) -> wiji.task.Task:
    class HttpTask(wiji.task.Task):
        async def run(self, *args, **kwargs):
            import aiohttp

            url = kwargs["url"]
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    print("resp statsus: ", resp.status)
                    res_text = await resp.text()
                    print(res_text[:50])

    task = HttpTask(the_broker=the_broker, queue_name="AsyncHttpQueue")
    return task


def print_task(the_broker) -> wiji.task.Task:
    class PrintTask(wiji.task.Task):
        async def run(self, *args, **kwargs):
            import hashlib

            print()
            print("RUNNING print_task:")
            print("args:", args)
            print("kwargs:", kwargs)
            print()
            h = hashlib.blake2b()
            h.update(b"Hello world")
            h.hexdigest()
            # await asyncio.sleep(0.4)

    task = PrintTask(the_broker=the_broker, queue_name="PrintQueue")
    return task


def adder_task(the_broker, chain=None) -> wiji.task.Task:
    class AdderTask(wiji.task.Task):
        async def run(self, a, b):
            res = a + b
            print()
            print("RUNNING adder_task:")
            print("adder: ", res)
            print()
            # await asyncio.sleep(2)
            if res in [10, 90]:
                await self.retry(a=221, b=555)
            return res

    task = AdderTask(the_broker=the_broker, queue_name="AdderTaskQueue", chain=chain)
    return task


def divider_task(the_broker, chain=None) -> wiji.task.Task:
    class DividerTask(wiji.task.Task):
        async def run(self, a):
            res = a / 3
            print()
            print("RUNNING divider_task:")
            print("divider: ", res)
            print()
            return res

    task = DividerTask(the_broker=the_broker, queue_name="DividerTaskQueue", chain=chain)
    return task


def multiplier_task(the_broker, chain=None) -> wiji.task.Task:
    class MultiplierTask(wiji.task.Task):
        async def run(self, bbb, a=5.5):
            res = bbb * a
            print()
            print("RUNNING multiplier_task:")
            print("multiplier: ", res)
            print()
            return res

    task = MultiplierTask(the_broker=the_broker, queue_name="MultiplierTaskQueue", chain=chain)
    return task


def exception_task(the_broker, chain=None) -> wiji.task.Task:
    class ExceptionTask(wiji.task.Task):
        async def run(self):
            print()
            print("RUNNING exception_task:")
            print()
            # await asyncio.sleep(0.5)
            raise ValueError("\n Houston We got 99 problems. \n")

    task = ExceptionTask(the_broker=the_broker, queue_name="ExceptionTaskQueue", chain=chain)
    return task


class ExampleRedisBroker(wiji.broker.BaseBroker):
    """
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
        self.redis_instance = redis.StrictRedis(host="localhost", port=6379, db=0)

    async def check(self, queue_name: str) -> None:
        await asyncio.sleep(1 / 117)
        pass

    async def enqueue(self, item: str, queue_name: str, task_options) -> None:
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix="wiji-redis-thread-pool"
        ) as executor:
            await self.loop.run_in_executor(
                executor, functools.partial(self.blocking_enqueue, queue_name=queue_name, item=item)
            )

    def blocking_enqueue(self, queue_name, item):
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
                    executor, functools.partial(self.blocking_dequeue, queue_name=queue_name)
                )
                if item:
                    return item
                else:
                    await asyncio.sleep(5)

    def blocking_dequeue(self, queue_name: str):
        dequed_item = self.redis_instance.brpop(queue_name, timeout=3)
        if not dequed_item:
            return None
        dequed_item = dequed_item[1]
        return dequed_item


MY_BROKER = ExampleRedisBroker()  # wiji.broker.SimpleBroker()
###############################################################################################

# 1. publish task

# ##### publish 1 ###############
multiplier = multiplier_task(the_broker=MY_BROKER)
divider = divider_task(the_broker=MY_BROKER, chain=multiplier)

adder = adder_task(the_broker=MY_BROKER, chain=divider)

adder.synchronous_delay(3, 7, task_options=wiji.task.TaskOptions(eta=4.56))
#############################################

# ALTERNATIVE way of chaining
adder = adder_task(the_broker=MY_BROKER)
divider = divider_task(the_broker=MY_BROKER)
multiplier = multiplier_task(the_broker=MY_BROKER)
adder | divider | multiplier

#####################################
http_task1 = http_task(the_broker=MY_BROKER)
http_task1.synchronous_delay(url="http://httpbin.org/get")

print_task2 = print_task(the_broker=MY_BROKER)
print_task2.synchronous_delay("myarg", my_kwarg="my_kwarg")

exception_task22 = exception_task(the_broker=MY_BROKER)
#####################################

BLOCKING_task = BLOCKING_http_task(the_broker=MY_BROKER)


async def task_producer(task, *args, **kwargs):
    while True:
        print()
        print("producing tasks..")
        await task.delay(*args, **kwargs)


if __name__ == "__main__":

    async def t():
        gather_tasks = asyncio.gather(
            task_producer(task=print_task2, my_KWARGS={"name": "Jay-Z", "age": 4040}),
            task_producer(task=adder, a=23, b=67),
            task_producer(task=http_task1, url="https://httpbin.org/delay/45"),
            task_producer(task=exception_task22, task_options=wiji.task.TaskOptions(eta=-34.99)),
            task_producer(
                task=BLOCKING_task,
                url="https://httpbin.org/delay/11",
                task_options=wiji.task.TaskOptions(eta=2.33),
            ),
        )
        await gather_tasks

    asyncio.run(t())
