# This code is taken from: https://github.com/python-trio/trio/pull/596/files
# Trio bills itself as; Trio – Pythonic async I/O for humans and snake people.
# Trio is released under a dual license of Apache or MIT
# 1. https://github.com/python-trio/trio/blob/master/LICENSE
# 2. https://github.com/python-trio/trio/blob/master/LICENSE.APACHE2
# 3. https://github.com/python-trio/trio/blob/master/LICENSE.MIT


import sys
import typing
import logging
import threading
import traceback


from . import logger

# TODO: This code is taken from a PR that has not been merged in Trio.
# We should monitor the tracking issue: https://github.com/python-trio/trio/issues/591
# Eventually we should adopt the solution that eventually gets merged into Trio(or alternatively; adopt Trio itself)


class BlockingTaskError(BlockingIOError):
    """
    Exception raised by wiji when the main asyncio thread is blocked by either
    an IO bound task execution or a CPU bound task execution.

    This Exception is raised in a separate thread(from the main asyncio thread) and thus does not
    impact the main thread.
    """

    pass


class BlockingWatchdog:
    """
    Monitors for any blocking calls in the main python asyncio thread.

    Python runs all asyncio operations(coroutines/tasks) on one Main thread in an evented manner.
    It is thus important that any operation you run in an asyncio environment be non-blocking.
    All your libraries(for http requests, database connections, rabbbitMQ clients etc) need to be async.
      As an example, the popular python http library/client; `python-requests <https://github.com/kennethreitz/requests>`_ is not async(non-blocking)
      For http requests, you should consider using an async client like `aiohttp <https://github.com/aio-libs/aiohttp>`_

    This class runs in a separate thread(away from the Main asyncio thread) so that it can monitor for any blocking calls on the Main thread.
    It does blocking detection in intervals of `watchdog_duration` seconds.
    The `watchdog_duration` is configurable and defaults to 0.1 seconds(0.1 seconds since that is also the default value in core Python.)
    We urge caution in trying to configure it to any value longer than 0.4 seconds;
      0.4s is the approximate `ping` duration(request & response) between the two farthest points on Earth for a signal travelling through copper.
      Of course, if you are doing any interstellar communication, then we urge you to consider using tools that are better suited for those
      kind of endeavours. `wiji` is not (as yet) suitable for interstellar IO communication.

    When `BlockingWatchdog` detects a blocking - IO/CPU bound - call that lasts for longer than `watchdog_duration` seconds;
    it will log an event that looks like:
        {
            "event": "wiji.BlockingWatchdog.blocked",
            "stage": "end",
            "error": "Blocked tasks Watchdog has not received any notifications in 0.1 seconds. This means the Main thread is blocked! "
            "Hint: are you running any tasks with blocking calls? eg; using python-requests, etc? "
            "Hint: look at the `stack_trace` attached to this log event to discover which calls are potentially blocking.",
            "stack_trace": [
                {
                    "thread_name": "MainThread",
                    "thread_stack_trace": [
                        "File cli/cli.py, line 269, in <module>\n    asyncio.run(async_main(), debug=True)\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/asyncio/runners.py, line 43, in run\n    return loop.run_until_complete(main)\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/asyncio/base_events.py, line 555, in run_until_complete\n    self.run_forever()\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/asyncio/base_events.py, line 523, in run_forever\n    self._run_once()\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/asyncio/base_events.py, line 1750, in _run_once\n    handle._run()\n",
                        "File /mystuff/wiji/wiji/worker.py, line 222, in consume_tasks\n    await self.run(*task_args, **task_kwargs)\n",
                        "File cli/cli.py, line 93, in run\n    resp = requests.get(url)\n",
                        "File /myVirtualenv/site-packages/requests/sessions.py, line 533, in request\n    resp = self.send(prep, **send_kwargs)\n",
                        "File /myVirtualenv/site-packages/requests/sessions.py, line 646, in send\n    r = adapter.send(request, **kwargs)\n",
                        "File /myVirtualenv/site-packages/requests/adapters.py, line 449, in send\n    timeout=timeout\n",
                        "File /myVirtualenv/site-packages/urllib3/connectionpool.py, line 600, in urlopen\n    chunked=chunked)\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/http/client.py, line 1321, in getresponse\n    response.begin()\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/socket.py, line 589, in readinto\n    return self._sock.recv_into(b)\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/ssl.py, line 1049, in recv_into\n    return self.read(nbytes, buffer)\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/ssl.py, line 908, in read\n    return self._sslobj.read(len, buffer)\n",
                    ],
                }
            ],
            "task_name": "_watchdogTask",
        }

    As you can see from that log event, you should as an application developer be able to identify where in your code the blocking calls are.
    When you do that, it is important you rectify them and make them async calls.
    If you do not, your task processing/execution is going to slow down considerably.

    If you buy into Python's asyncio world(which I think you should if your tasks/operations are mostly IO-bound), you should accept the fact that
    Python is running your stuff on one Thread.
    Your life will be much happier once you accept this and architect your operations with that in mind.
    """

    def __init__(self, watchdog_duration: float, task_name: str) -> None:
        if not isinstance(watchdog_duration, float):
            raise ValueError(
                """`watchdog_duration` should be of type:: `float` You entered: {0}""".format(
                    type(watchdog_duration)
                )
            )
        if not isinstance(task_name, str):
            raise ValueError(
                """`task_name` should be of type:: `str` You entered: {0}""".format(type(task_name))
            )

        self.watchdog_duration = watchdog_duration
        self.task_name = task_name

        self._stopped: bool = False
        self._thread: typing.Union[None, threading.Thread] = None
        self._notify_event: threading.Event = threading.Event()
        self._before_counter: int = 0
        self._after_counter: int = 0

        self.logger: logger.BaseLogger = logger.SimpleLogger("wiji.BlockingWatchdog")
        self.logger.bind(level="WARNING", log_metadata={"task_name": self.task_name})

    def notify_alive_before(self) -> None:
        """
        Notifies the watchdog that the Worker thread(the Event loop) is alive before running
        a task.
        """
        self._before_counter += 1
        self._notify_event.set()

    def notify_alive_after(self) -> None:
        """
        Notifies the watchdog that the Worker thread(the Event loop) is alive after running a
        task.
        """
        self._after_counter += 1

    def _main_loop(self) -> None:
        """
        Raises:
            BlockingTaskError: Exception raised when the main asyncio thread is blocked by either an IO/CPU bound task execution.
        """
        while True:
            if self._stopped:
                return

            self._notify_event.clear()
            orig_starts = self._before_counter
            orig_stops = self._after_counter

            if orig_starts == orig_stops:
                # main thread asleep; nothing to do until it wakes up
                self._notify_event.wait()
                if self._stopped:
                    return
            else:
                self._notify_event.wait(timeout=self.watchdog_duration)
                if self._stopped:
                    return

                if orig_starts == self._before_counter and orig_stops == self._after_counter:
                    try:
                        error_msg = (
                            "Blocked tasks Watchdog has not received any notifications in {watchdog_duration} seconds. "
                            "This means the Main thread is blocked! "
                            "\nHint: are you running any tasks with blocking calls? eg; using python-requests, etc? "
                            "\nHint: look at the `stack_trace` attached to this log event to discover which calls are potentially blocking.".format(
                                watchdog_duration=self.watchdog_duration
                            )
                        )
                        raise BlockingTaskError(error_msg)
                    except Exception as e:
                        all_threads_stack_trace = self._save_stack_trace()
                        # TODO: maybe we should log this at WARNING level, or DEBUG??
                        self.logger.log(
                            logging.ERROR,
                            {
                                "event": "wiji.BlockingWatchdog.blocked",
                                "stage": "end",
                                "error": str(e),
                                "stack_trace": all_threads_stack_trace,
                            },
                        )

    def _save_stack_trace(self) -> list:
        """
        This method returns a list of dictionaries each of which contains a thread name and its corresponding stack_trace.
        It returns something like:
            [
                {
                    "thread_name": "MainThread",
                    "thread_stack_trace": [
                        "File cli/cli.py, line 269, in <module>\n    asyncio.run(async_main(), debug=True)\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/asyncio/runners.py, line 43, in run\n    return loop.run_until_complete(main)\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/asyncio/base_events.py, line 555, in run_until_complete\n    self.run_forever()\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/asyncio/base_events.py, line 523, in run_forever\n    self._run_once()\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/asyncio/base_events.py, line 1750, in _run_once\n    handle._run()\n",
                        "File /mystuff/wiji/wiji/worker.py, line 222, in consume_tasks\n    await self.run(*task_args, **task_kwargs)\n",
                        "File cli/cli.py, line 93, in run\n    resp = requests.get(url)\n",
                        "File /myVirtualenv/site-packages/requests/sessions.py, line 533, in request\n    resp = self.send(prep, **send_kwargs)\n",
                        "File /myVirtualenv/site-packages/requests/sessions.py, line 646, in send\n    r = adapter.send(request, **kwargs)\n",
                        "File /myVirtualenv/site-packages/requests/adapters.py, line 449, in send\n    timeout=timeout\n",
                        "File /myVirtualenv/site-packages/urllib3/connectionpool.py, line 600, in urlopen\n    chunked=chunked)\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/http/client.py, line 1321, in getresponse\n    response.begin()\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/socket.py, line 589, in readinto\n    return self._sock.recv_into(b)\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/ssl.py, line 1049, in recv_into\n    return self.read(nbytes, buffer)\n",
                        "File /usr/python/3.7.0/3.7/lib/python3.7/ssl.py, line 908, in read\n    return self._sslobj.read(len, buffer)\n"
                    ]
                },
            ]
        From such a stack-trace, we can clearly see that we are using python requests to make network calls.
        And since python requests is not async, this is blocking the python event loop thread and thus slowing everything down.
        """
        # we could also use: faulthandler.dump_traceback(all_threads=True)
        stack_trace_of_all_threads_during_block = []
        for thread in threading.enumerate():
            thread_stack_trace = traceback.format_stack(
                f=sys._current_frames()[thread.ident]  # type: ignore
            )
            thread_name = thread.name
            if thread_name == "MainThread":
                # we are only interested in blocking ops in the asyncio thread
                stack_trace_of_all_threads_during_block.append(
                    {"thread_name": thread_name, "thread_stack_trace": thread_stack_trace}
                )
        return stack_trace_of_all_threads_during_block

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._main_loop, name="Thread-<wiji_watchdog>", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        if typing.TYPE_CHECKING:
            # make mypy happy
            # https://github.com/python/mypy/issues/4805
            assert isinstance(self._thread, threading.Thread)
        self._stopped = True
        self._notify_event.set()
        self._thread.join()
