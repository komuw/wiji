import os
import uuid
import json
import random
import string
import typing
import logging
import asyncio
import datetime

from . import q
from . import hooks
from . import logger
from . import ratelimiter


class Worker:
    """
    """

    def __init__(
        self,
        async_loop: asyncio.events.AbstractEventLoop,
        queue: q.BaseQueue,
        rateLimiter=None,
        hook=None,
        Worker_id=None,
        log_handler=None,
        loglevel: str = "DEBUG",
        log_metadata=None,
    ) -> None:
        """
        """
        if loglevel.upper() not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(
                """loglevel should be one of; 'DEBUG', 'INFO', 'WARNING', 'ERROR' or 'CRITICAL'. not {0}""".format(
                    loglevel
                )
            )
        if not isinstance(log_metadata, (type(None), dict)):
            raise ValueError(
                """log_metadata should be of type:: None or dict. You entered {0}""".format(
                    type(log_metadata)
                )
            )

        self.async_loop = async_loop
        self.loglevel = loglevel.upper()
        self.queue = queue

        self.Worker_id = Worker_id
        if not self.Worker_id:
            self.Worker_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))

        self.log_metadata = log_metadata
        if not self.log_metadata:
            self.log_metadata = {}
        self.log_metadata.update({"Worker_id": self.Worker_id})

        self.logger = log_handler
        if not self.logger:
            self.logger = logger.SimpleBaseLogger("xyzabc.Worker")
        self.logger.bind(loglevel=self.loglevel, log_metadata=self.log_metadata)
        self._sanity_check_logger()

        self.rateLimiter = rateLimiter
        if not self.rateLimiter:
            self.rateLimiter = ratelimiter.SimpleRateLimiter(logger=self.logger)

        self.hook = hook
        if not self.hook:
            self.hook = hooks.SimpleHook(logger=self.logger)

    def _sanity_check_logger(self):
        """
        called when instantiating the Worker just to make sure the supplied
        logger can log.
        """
        try:
            self.logger.log(logging.DEBUG, {"event": "sanity_check_logger"})
        except Exception as e:
            raise e

    def _log(self, level, log_data):
        # if the supplied logger is unable to log; we move on
        try:
            self.logger.log(level, log_data)
        except Exception:
            pass

    @staticmethod
    def _retry_after(current_retries):
        """
        retries will happen in this sequence;
        1min, 2min, 4min, 8min, 16min, 32min, 16min, 16min, 16min ...
        """
        # TODO:
        # 1. give users ability to bring their own retry algorithms.
        # 2. add jitter
        if current_retries < 0:
            current_retries = 0
        if current_retries >= 6:
            return 60 * 16  # 16 minutes
        else:
            return 60 * (1 * (2 ** current_retries))

    async def run(self, pickled_obj):
        # this is where people put their code.
        import pickle

        unpickled_obj = pickle.loads(pickled_obj)
        unpickled_obj()
        pass

    async def send_forever(
        self, TESTING: bool = False
    ) -> typing.Union[str, typing.Dict[typing.Any, typing.Any]]:
        """
        In loop; dequeues items from the :attr:`queue <Worker.queue>` and calls :func:`run <Worker.run>`.

        Parameters:
            TESTING: indicates whether this method is been called while running tests.
        """
        retry_count = 0
        while True:
            self._log(logging.INFO, {"event": "xyzabc.Worker.send_forever", "stage": "start"})

            try:
                # rate limit ourselves
                await self.rateLimiter.limit()
            except Exception as e:
                self._log(
                    logging.ERROR,
                    {
                        "event": "xyzabc.Worker.send_forever",
                        "stage": "end",
                        "state": "send_forever error",
                        "error": str(e),
                    },
                )
                continue

            try:
                item_to_dequeue = await self.queue.dequeue()
            except Exception as e:
                retry_count += 1
                poll_queue_interval = self._retry_after(retry_count)
                self._log(
                    logging.ERROR,
                    {
                        "event": "xyzabc.Worker.send_forever",
                        "stage": "end",
                        "state": "send_forever error. sleeping for {0}minutes".format(
                            poll_queue_interval / 60
                        ),
                        "retry_count": retry_count,
                        "error": str(e),
                    },
                )
                await asyncio.sleep(poll_queue_interval)
                continue

            # dequeue succeded
            retry_count = 0
            try:
                log_id = item_to_dequeue["log_id"]
                item_to_dequeue["version"]  # version is a required field
                smpp_command = item_to_dequeue["smpp_command"]
                hook_metadata = item_to_dequeue.get("hook_metadata", "")
            except KeyError as e:
                e = KeyError("enqueued message/object is missing required field:{}".format(str(e)))
                self._log(
                    logging.ERROR,
                    {
                        "event": "xyzabc.Worker.send_forever",
                        "stage": "end",
                        "state": "send_forever error",
                        "error": str(e),
                    },
                )
                continue

            ###################### example ##########
            import pickle

            def hello_func():
                print("\n hello_func called \n")

            pickled_obj = pickle.dumps(hello_func, pickle.HIGHEST_PROTOCOL)
            #########################################

            await self.run(pickled_obj)
            self._log(
                logging.INFO,
                {
                    "event": "xyzabc.Worker.send_forever",
                    "stage": "end",
                    "log_id": log_id,
                    "smpp_command": smpp_command,
                },
            )
            if TESTING:
                # offer escape hatch for tests to come out of endless loop
                return item_to_dequeue


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
        queue = q.SimpleOutboundQueue()
        loop.run_until_complete(queue.enqueue(item=protocol_json, queue_name=task_options.queue))

        print(protocol)
        print()
        print(json.dumps(protocol, indent=2))
