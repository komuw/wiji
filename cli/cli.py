import os
import sys
import json
import string
import signal
import random
import typing
import asyncio
import inspect
import logging
import argparse
import functools

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


# TODO: this functions should live in their own file
async def _signal_handling(workers):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()

    try:
        for signal_number in [signal.SIGHUP, signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]:
            loop.add_signal_handler(
                signal_number,
                functools.partial(
                    asyncio.ensure_future,
                    _handle_termination_signal(signal_number=signal_number, workers=workers),
                ),
            )
    except ValueError as e:
        # TODO: add debug logging
        print("this OS does not support the said signal")


async def _handle_termination_signal(signal_number, workers):
    signal_table = {1: "SIGHUP", 2: "SIGINT", 3: "SIGQUIT", 9: "SIGKILL", 15: "SIGTERM"}
    # TODO: add debug logging

    shutdown_tasks = []
    for worker in workers:
        shutdown_tasks.append(worker.shutdown())
    # the shutdown process for all tasks needs to happen concurrently
    tasks = asyncio.gather(*shutdown_tasks)
    asyncio.ensure_future(tasks)

    def wait_worker_shutdown(workers) -> bool:
        no_workers = len(workers)
        success_shut_down = 0
        for worker in workers:
            if worker.SUCCESFULLY_SHUT_DOWN:
                success_shut_down += 1

        if success_shut_down == no_workers:
            loop_continue = False
        else:
            loop_continue = True
        return loop_continue

    while wait_worker_shutdown(workers=workers):
        print()
        print("wiji-cli: waiting for all workers to drain properly....")
        print()
        await asyncio.sleep(5)

    print()
    print("wiji-cli: shuttting down all workers was achieved succesfully.")
    print()
    return


def main():
    """
    """
    pass


async def produce_tasks_continously(task, *args, **kwargs):
    while True:
        await task.delay(*args, **kwargs)


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
            await asyncio.sleep(0.4)

    task = PrintTask(the_broker=the_broker, queue_name="PrintQueue")
    return task


################## CHAIN ##################
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


################## CHAIN ##################


def exception_task(the_broker, chain=None) -> wiji.task.Task:
    class ExceptionTask(wiji.task.Task):
        async def run(self):
            print()
            print("RUNNING exception_task:")
            print()
            await asyncio.sleep(0.5)
            raise ValueError("\n Houston We got 99 problems. \n")

    task = ExceptionTask(the_broker=the_broker, queue_name="ExceptionTaskQueue", chain=chain)
    return task


if __name__ == "__main__":
    main()
    """
    run as:
        python cli/cli.py
    """

    MY_BROKER = wiji.broker.InMemoryBroker()

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

    all_tasks = [
        http_task1,
        print_task2,
        adder,
        divider,
        multiplier,
        exception_task22,
        BLOCKING_task,
    ]

    workers = [wiji.Worker(the_task=wiji.task.WatchDogTask, use_watchdog=True)]
    producers = [produce_tasks_continously(task=wiji.task.WatchDogTask)]

    for task in all_tasks:
        _worker = wiji.Worker(the_task=task)
        workers.append(_worker)

    consumers = []
    for i in workers:
        consumers.append(i.consume_tasks())

    producers.extend(
        [
            produce_tasks_continously(task=http_task1, url="https://httpbin.org/delay/45"),
            produce_tasks_continously(task=print_task2, my_KWARGS={"name": "Jay-Z", "age": 4040}),
            produce_tasks_continously(task=adder, a=23, b=67),
            produce_tasks_continously(
                task=exception_task22, task_options=wiji.task.TaskOptions(eta=-34.99)
            ),
            produce_tasks_continously(
                task=BLOCKING_task,
                url="https://httpbin.org/delay/11",
                task_options=wiji.task.TaskOptions(eta=2.33),
            ),
        ]
    )

    # 2.consume tasks
    async def async_main():
        wiji_pid = workers[0]._PID
        print("PID", wiji_pid)
        gather_tasks = asyncio.gather(*consumers, *producers, _signal_handling(workers))
        await gather_tasks

    asyncio.run(async_main(), debug=True)
