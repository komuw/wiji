# This code is taken from: https://github.com/python-trio/trio/pull/596/files
# Trio bills itself as; Trio – Pythonic async I/O for humans and snake people.
# Trio is released under a dual license of Apache or MIT
# 1. https://github.com/python-trio/trio/blob/master/LICENSE
# 2. https://github.com/python-trio/trio/blob/master/LICENSE.APACHE2
# 3. https://github.com/python-trio/trio/blob/master/LICENSE.MIT


import sys
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


class _BlocingWatchdog:
    """
    monitors for any blocking calls in an otherwise async coroutine.
    """

    def __init__(self, watchdog_timeout: float, task_name: str):
        if not isinstance(watchdog_timeout, float):
            raise ValueError(
                """`watchdog_timeout` should be of type:: `float` You entered: {0}""".format(
                    type(watchdog_timeout)
                )
            )
        if not isinstance(task_name, str):
            raise ValueError(
                """`task_name` should be of type:: `str` You entered: {0}""".format(type(task_name))
            )

        self.watchdog_timeout = watchdog_timeout
        self.task_name = task_name

        self._stopped = False
        self._thread = None
        self._notify_event = threading.Event()
        self._before_counter = 0
        self._after_counter = 0

        self.logger = logger.SimpleBaseLogger("wiji._BlocingWatchdog")
        self.logger.bind(loglevel="DEBUG", log_metadata={"task_name": self.task_name})

    def notify_alive_before(self):
        """
        Notifies the watchdog that the Worker thread(the Event loop) is alive before running
        a task.
        """
        self._before_counter += 1
        self._notify_event.set()

    def notify_alive_after(self):
        """
        Notifies the watchdog that the Worker thread(the Event loop) is alive after running a
        task.
        """
        self._after_counter += 1

    def _main_loop(self):
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
                self._notify_event.wait(timeout=self.watchdog_timeout)
                if self._stopped:
                    return

                if orig_starts == self._before_counter and orig_stops == self._after_counter:
                    try:
                        error_msg = (
                            "ERROR: blocked tasks Watchdog has not received any notifications in {watchdog_timeout} seconds. "
                            "This means the Main thread is blocked! "
                            "\nHint: are you running any tasks with blocking calls? eg; using python-requests? etc? "
                            "\nHint: look at the `stack_trace` attached to this log event to discover which calls are potentially blocking.".format(
                                watchdog_timeout=self.watchdog_timeout
                            )
                        )
                        raise BlockingTaskError(error_msg)
                    except Exception as e:
                        all_threads_stack_trace = self._save_stack_trace()
                        self.logger.log(
                            logging.ERROR,
                            {
                                "event": "wiji._BlocingWatchdog.blocked",
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
                        "File /mystuff/wiji/wiji/worker.py, line 222, in consume_forever\n    await self.run(*task_args, **task_kwargs)\n",
                        "File cli/cli.py, line 93, in async_run\n    resp = requests.get(url)\n",
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
        """
        # we could also use: faulthandler.dump_traceback(all_threads=True)
        stack_trace_of_all_threads_during_block = []
        for thread in threading.enumerate():
            thread_stack_trace = traceback.format_stack(f=sys._current_frames()[thread.ident])
            stack_trace_of_all_threads_during_block.append(
                {"thread_name": thread.name, "thread_stack_trace": thread_stack_trace}
            )
            return stack_trace_of_all_threads_during_block

    def start(self):
        self._thread = threading.Thread(
            target=self._main_loop, name="Thread-<wiji_watchdog>", daemon=True
        )
        self._thread.start()

    def stop(self):
        self._stopped = True
        self._notify_event.set()
        self._thread.join()
