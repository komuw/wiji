import os
import uuid
import json
import asyncio
import datetime

from . import q


class TaskOptions:
    def __init__(self, eta, retries, queue_name, file_name, class_path, log_id, hook_metadata):
        """
        Parameters:
            eta: Number of seconds into the future that the task should execute.  Defaults to immediate execution.
            retries:
            queue_name: The queue to route the task to.
        """
        self.eta = eta
        self.retries = retries
        self.queue_name = queue_name
        self.file_name = file_name
        self.class_path = class_path
        self.log_id = log_id
        self.hook_metadata = hook_metadata


class Task:
    """
    call it as:
        Task()(33,"hello", name="komu")

    usage:
        opt = TaskOptions(eta=60,
                          retries=3,
                          queue_name="myQueue",
                          file_name=__file__,
                          class_path=os.path.realpath(__file__)
                        )
        task = Task()
        task.delay(33, "hello", name="komu", task_options=opt)
    """

    def __init__(self, queue: q.BaseQueue):
        self.queue = queue

    def __call__(self, *args, **kwargs):
        self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        print(args)
        print(kwargs)
        print("ssdsd")

    async def delay(self, *args, **kwargs):
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
            "queue_name": task_options.queue_name,
            "file_name": task_options.file_name,
            "class_path": task_options.class_path,
            "log_id": task_options.log_id,
            "hook_metadata": task_options.hook_metadata,
            "timelimit": 1800,
            "args": args,
            "kwargs": kwargs,
        }

        protocol_json = json.dumps(protocol)
        await self.queue.enqueue(item=protocol_json, queue_name=task_options.queue_name)
        print(protocol)
        print()
        print(json.dumps(protocol, indent=2))

    def blocking_delay(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.delay(*args, **kwargs))
