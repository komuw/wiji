import os
import sys
import json
import random
import string
import asyncio
import typing
import logging
import inspect
import argparse


import xyzabc

os.environ["PYTHONASYNCIODEBUG"] = "1"


def load_class(dotted_path):
    """
    taken from: https://github.com/coleifer/huey/blob/4138d454cc6fd4d252c9350dbd88d74dd3c67dcb/huey/utils.py#L44
    huey is released under MIT license a copy of which can be found at: https://github.com/coleifer/huey/blob/master/LICENSE

    The license is also included below:

    Copyright (c) 2017 Charles Leifer

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.
    """
    try:
        path, klass = dotted_path.rsplit(".", 1)
        __import__(path)
        mod = sys.modules[path]
        attttr = getattr(mod, klass)
        return attttr
    except Exception:
        cur_dir = os.getcwd()
        if cur_dir not in sys.path:
            sys.path.insert(0, cur_dir)
            return load_class(dotted_path)
        err_mesage = "Error importing {0}".format(dotted_path)
        sys.stderr.write("\033[91m{0}\033[0m\n".format(err_mesage))
        raise


def main():
    """
    """
    pass


async def produce_tasks_continously(task, *args, **kwargs):
    import random

    while True:
        await task.async_delay(*args, **kwargs)
        await asyncio.sleep(random.randint(7, 14))


def http_task(broker) -> xyzabc.task.Task:
    class MyTask(xyzabc.task.Task):
        async def async_run(self, *args, **kwargs):
            import aiohttp

            url = kwargs["url"]
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    print("resp statsus: ", resp.status)
                    res_text = await resp.text()
                    print(res_text[:50])

    opt = xyzabc.task.TaskOptions(
        broker=broker,
        queue_name="HttpQueue",
        eta=60,
        retries=3,
        file_name=__file__,
        class_path=os.path.realpath(__file__),
        log_id="myLogID",
        hook_metadata='{"email": "example@example.com"}',
    )
    task = MyTask(task_options=opt)
    return task


def print_task(broker) -> xyzabc.task.Task:
    class MyTask(xyzabc.task.Task):
        async def async_run(self, *args, **kwargs):
            print()
            print("args:", args)
            print("kwargs:", kwargs)
            print()

    opt = xyzabc.task.TaskOptions(
        broker=broker,
        queue_name="PrintQueue",
        eta=60,
        retries=3,
        file_name=__file__,
        class_path=os.path.realpath(__file__),
        log_id="myLogID",
        hook_metadata='{"email": "example@example.com"}',
    )
    task = MyTask(task_options=opt)
    return task


if __name__ == "__main__":
    main()
    """
    run as:
        python cli/cli.py
    """

    MY_BROKER = xyzabc.broker.SimpleBroker()
    TASKS: typing.List[xyzabc.task.Task] = []

    # 1. publish task

    ##### publish 1 ###############
    task1 = http_task(broker=MY_BROKER)
    task1.blocking_delay(url="http://httpbin.org/get")
    #############################################

    #### publish 2 #######################
    task2 = print_task(broker=MY_BROKER)
    task2.blocking_delay("myarg", my_kwarg="my_kwarg")
    #####################################

    # 2.consume task
    loop = asyncio.get_event_loop()
    TASKS.append(task1)
    TASKS.append(task2)
    worker1 = xyzabc.Worker(async_loop=loop, task=task1)
    worker2 = xyzabc.Worker(async_loop=loop, task=task2)

    tasks = asyncio.gather(
        worker1.consume_forever(),
        produce_tasks_continously(task=task1, url="http://httpbin.org/get"),
        produce_tasks_continously(task=task2, my_kwarg="my_kwarg2"),
        worker2.consume_forever(),
    )
    loop.run_until_complete(tasks)
