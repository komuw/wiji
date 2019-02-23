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


class _BlocingWatchdog:
    """
    monitors for any blocking calls in an otherwise async coroutine.
    """

    def __init__(self, timeout, task_name):
        self._timeout = timeout
        self.task_name = task_name

        self._stopped = False
        self._thread = None
        self._notify_event = threading.Event()
        self._before_counter = 0
        self._after_counter = 0

        self.logger = logger.SimpleBaseLogger("xyzabc._BlocingWatchdog")
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
                self._notify_event.wait(timeout=self._timeout)
                if self._stopped:
                    return

                if orig_starts == self._before_counter and orig_stops == self._after_counter:
                    try:
                        error_msg = (
                            "ERROR: blocked tasks Watchdog has not received any notifications in {timeout} seconds. This means the Main thread is blocked! "
                            "\nHint: are you running any blocking calls? using python-requests? etc? "
                            "\nHint: look at the `stack_trace` attached to this log event to discover which calls are potentially blocking.".format(
                                timeout=self._timeout
                            )
                        )
                        raise BlockingIOError(error_msg)
                    except Exception as e:
                        all_threads_stack_trace = self._save_stack_trace()
                        self.logger.log(
                            logging.ERROR,
                            {
                                "event": "xyzabc._BlocingWatchdog.blocked",
                                "stage": "end",
                                "error": str(e),
                                "stack_trace": all_threads_stack_trace,
                            },
                        )

    def _save_stack_trace(self):
        # we could also use: faulthandler.dump_traceback(all_threads=True)
        stack_trace_of_all_threads_during_block = []
        for thread in threading.enumerate():
            daara = traceback.format_stack(f=sys._current_frames()[thread.ident])
            stack_trace_of_all_threads_during_block.append(daara)
            return stack_trace_of_all_threads_during_block

    def start(self):
        self._thread = threading.Thread(
            target=self._main_loop, name="<xyzabc watchdog>", daemon=True
        )
        self._thread.start()

    def stop(self):
        self._stopped = True
        self._notify_event.set()
        self._thread.join()
