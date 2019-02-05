import os
import sys
import json
import random
import string
import asyncio
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


if __name__ == "__main__":
    main()
    """
    run as:
        python cli/cli.py 
    """
    # import pdb

    # pdb.set_trace()
    MY_QUEUE = xyzabc.q.SimpleQueue()
    queue_name = "myQueue"

    # 1. publish task
    opt = xyzabc.task.TaskOptions(
        eta=60,
        retries=3,
        queue_name=queue_name,
        file_name=__file__,
        class_path=os.path.realpath(__file__),
        log_id="myLogID",
        hook_metadata='{"email": "example@example.com"}',
    )

    class MyTask(xyzabc.task.Task):
        def run(self, *args, **kwargs):
            print("args: ", args)
            print("kwargs: ", kwargs)
            print("executing MyTask")

    # task = xyzabc.task.Task(queue=MY_QUEUE)
    task = MyTask(queue=MY_QUEUE)
    task.blocking_delay(33, "hello", name="komu", task_options=opt)
    import pdb

    pdb.set_trace()

    # 2.consume task
    loop = asyncio.get_event_loop()
    worker = xyzabc.Worker(async_loop=loop, queue=MY_QUEUE, queue_name=queue_name)

    tasks = asyncio.gather(
        worker.consume_forever(), task.delay(77, "hello22", name="KOMU2", task_options=opt)
    )
    loop.run_until_complete(tasks)
