import uuid
import json
import datetime
import os, sys
import asyncio
import typing


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


class SimpleOutboundQueue:
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
        else:
            self.store[queue_name] = [item]

    async def dequeue(self, queue_name: str) -> str:
        if self.store.get(queue_name):
            try:
                return await asyncio.sleep(delay=-1, result=self.store[queue_name].pop(0))
            except IndexError:
                return await asyncio.sleep(delay=-1, result=None)
        else:
            return await asyncio.sleep(delay=-1, result=None)


class TaskOptions:
    def __init__(self, eta, retries, queue, file_name, class_path):
        """
        Parameters:
            eta: Number of seconds into the future that the task should execute.  Defaults to immediate execution.
            retries:
            queue: The queue to route the task to.
        """
        self.eta = eta
        self.retries = retries
        self.queue = queue
        self.file_name = file_name
        self.class_path = class_path


class Task:
    """
    call it as:
        Task()(33,"hello", name="komu")

    usage:
        opt = TaskOptions(eta=60,
                          retries=3,
                          queue="myQueue",
                          file_name=__file__,
                          class_path=os.path.realpath(__file__)
                        )
        task = Task()
        task.delay(33, "hello", name="komu", task_options=opt)
    """

    def __call__(self, *args, **kwargs):
        self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        print(args)
        print(kwargs)
        print("ssdsd")

    # TODO make this an async func
    def delay(self, *args, **kwargs):
        """
        Parameters:
            args: The positional arguments to pass on to the task.
            kwargs: The keyword arguments to pass on to the task.
        """
        # Queue this to queue

        class_name: str = self.__class__.__name__

        task_options = kwargs.pop("task_options", None)
        task_options.class_path = task_options.class_path.replace(".py", "")
        task_options.class_path = (
            os.path.join(task_options.class_path, class_name).replace("/", ".").lstrip(".")
        )

        task_options.file_name = task_options.file_name.replace(".py", "")
        task_options.file_name = (
            os.path.join(task_options.file_name, class_name).replace("/", ".").lstrip(".")
        )

        eta = datetime.datetime.utcnow() + datetime.timedelta(seconds=task_options.eta)
        protocol = {
            "version": 1,
            "task_id": str(uuid.uuid4()),
            "eta": eta.isoformat(),
            "retries": task_options.retries,
            "queue": task_options.queue,
            "file_name": task_options.file_name,
            "class_path": task_options.class_path,
            "timelimit": 1800,
            "args": args,
            "kwargs": kwargs,
        }

        protocol_json = json.dumps(protocol)

        loop = asyncio.get_event_loop()
        queue = SimpleOutboundQueue()
        loop.run_until_complete(queue.enqueue(item=protocol_json, queue_name=task_options.queue))

        item = loop.run_until_complete(queue.dequeue(queue_name=task_options.queue))
        item_dict = json.loads(item)

        print(protocol)
        print()
        print(json.dumps(protocol, indent=2))


opt = TaskOptions(
    eta=60, retries=3, queue="myQueue", file_name=__file__, class_path=os.path.realpath(__file__)
)
task = Task()
task.delay(33, "hello", name="komu", task_options=opt)
