import asyncio
import hashlib
import subprocess

import wiji
import aiohttp
import requests


from examples.redis_broker import ExampleRedisBroker


MY_BROKER = ExampleRedisBroker()  # wiji.broker.SimpleBroker()


class BlockingDiskIOTask(wiji.task.Task):
    the_broker = MY_BROKER
    queue_name = "BlockingDiskIOTaskQueue"

    async def run(self, *args, **kwargs):
        print()
        print("RUNNING BlockingDiskIOTask:")
        subprocess.run(["dd", "if=/dev/zero", "of=/dev/null", "bs=500000", "count=1000000"])


class BlockinHttpTask(wiji.task.Task):
    the_broker = MY_BROKER
    queue_name = "BlockinHttpTaskQueue"

    async def run(self, *args, **kwargs):
        print()
        print("RUNNING BLOCKING_http_task:")
        url = kwargs["url"]
        resp = requests.get(url)
        print("resp: ", resp)


class AsyncHttpTask(wiji.task.Task):
    the_broker = MY_BROKER
    queue_name = "AsyncHttpTaskQueue"

    async def run(self, *args, **kwargs):
        url = kwargs["url"]
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                print("resp statsus: ", resp.status)
                res_text = await resp.text()
                print(res_text[:50])


class PrintTask(wiji.task.Task):
    the_broker = MY_BROKER
    queue_name = "PrintTaskQueue"

    async def run(self, *args, **kwargs):
        print()
        print("RUNNING print_task:")
        print("args:", args)
        print("kwargs:", kwargs)
        print()
        h = hashlib.blake2b()
        h.update(b"Hello world")
        h.hexdigest()
        # await asyncio.sleep(0.4)


class MultiplierTask(wiji.task.Task):
    the_broker = MY_BROKER
    queue_name = "MultiplierTaskQueue"

    async def run(self, bbb, a=5.5):
        res = bbb * a
        print()
        print("RUNNING multiplier_task:")
        print("multiplier: ", res)
        print()
        return res


class DividerTask(wiji.task.Task):
    the_broker = MY_BROKER
    queue_name = "DividerTaskQueue"
    # TODO: we should able to use a class instead of instance as chain
    chain = MultiplierTask()

    async def run(self, a):
        res = a / 3
        print()
        print("RUNNING divider_task:")
        print("divider: ", res)
        print()
        return res


class AdderTask(wiji.task.Task):
    the_broker = MY_BROKER
    queue_name = "AdderTaskQueue"
    chain = DividerTask()

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


class ExceptionTask(wiji.task.Task):
    the_broker = MY_BROKER
    queue_name = "ExceptionTaskQueue"

    async def run(self):
        print()
        print("RUNNING exception_task:")
        print()
        # await asyncio.sleep(0.5)
        raise ValueError("\n Houston We got 99 problems. \n")


# 1. publish intial tasks
adder = AdderTask()
adder.synchronous_delay(3, 7, task_options=wiji.task.TaskOptions(eta=4.56))


# 2. publish  tasks continuously
async def task_producer(task_class, *args, **kwargs):
    while True:
        print()
        print("producing tasks..")
        task = task_class()
        await task.delay(*args, **kwargs)


if __name__ == "__main__":

    async def t():
        gather_tasks = asyncio.gather(
            task_producer(task_class=PrintTask, my_KWARGS={"name": "Jay-Z", "age": 4040}),
            task_producer(task_class=AdderTask, a=23, b=67),
            task_producer(task_class=AsyncHttpTask, url="https://httpbin.org/delay/45"),
            task_producer(task_class=ExceptionTask, task_options=wiji.task.TaskOptions(eta=-34.99)),
            task_producer(
                task_class=BlockinHttpTask,
                url="https://httpbin.org/delay/11",
                task_options=wiji.task.TaskOptions(eta=2.33),
            ),
        )
        await gather_tasks

    asyncio.run(t())
