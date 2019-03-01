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


import wiji

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
        err_message = "Error importing {0}".format(dotted_path)
        sys.stderr.write("\033[91m{0}\033[0m\n".format(err_message))
        raise


def main():
    """
    """
    pass


async def produce_tasks_continously(task, *args, **kwargs):
    while True:
        await task.delay(*args, **kwargs)


def adder_task(the_broker, chain=None) -> wiji.task.Task:
    class AdderTask(wiji.task.Task):
        async def run(self, a, b):
            res = a + b
            print()
            print("RUNNING adder_task:")
            print("adder: ", res)
            print()
            await asyncio.sleep(2)
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


if __name__ == "__main__":
    main()
    """
    run as:
        python cli/cli.py
    """

    MY_BROKER = wiji.broker.SimpleBroker()

    # 1. publish task

    # ##### publish 1 ###############
    divider = divider_task(the_broker=MY_BROKER)

    adder = adder_task(the_broker=MY_BROKER, chain=divider)

    adder.synchronous_delay(3, 7, task_options=wiji.task.TaskOptions(eta=0.01, max_retries=3))
    # adder.synchronous_delay(a=23, b=67)

    all_tasks = [adder, divider]

    workers = []  # [wiji.Worker(the_task=wiji.task.WatchDogTask, use_watchdog=True)]
    producers = []  # [produce_tasks_continously(task=wiji.task.WatchDogTask)]

    for task in all_tasks:
        _worker = wiji.Worker(the_task=task)
        workers.append(_worker)

    consumers = []
    for i in workers:
        consumers.append(i.consume_forever())

    # producers.extend([produce_tasks_continously(task=adder, a=23, b=67)])

    # 2.consume tasks
    async def async_main():
        gather_tasks = asyncio.gather(*consumers, *producers)
        await gather_tasks

    asyncio.run(async_main(), debug=True)
