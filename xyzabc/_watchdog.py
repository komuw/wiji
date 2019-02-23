# This code is taken from: https://github.com/python-trio/trio/pull/596/files
# Trio bills itself as; Trio – Pythonic async I/O for humans and snake people.
# Trio is released under a dual license of Apache or MIT
# 1. https://github.com/python-trio/trio/blob/master/LICENSE
# 2. https://github.com/python-trio/trio/blob/master/LICENSE.APACHE2
# 3. https://github.com/python-trio/trio/blob/master/LICENSE.MIT


import sys

import threading
import traceback


# TODO: This code is taken from a PR that has not been merged in Trio.
# We should monitor the tracking issue: https://github.com/python-trio/trio/issues/591
# Eventually we should adopt the solution that eventually gets merged into Trio(or alternatively; adopt Trio itself)


class _BlocingWatchdog(object):
    """
    monitors for any blocking calls in an otherwise async coroutine.
    """

    def __init__(self, timeout=5):
        self._stopped = False
        self._thread = None
        self._notify_event = threading.Event()
        self._timeout = timeout

        self._before_counter = 0
        self._after_counter = 0

    def notify_alive_before(self):
        """
        Notifies the watchdog that the Trio thread is alive before running
        a task.
        """
        self._before_counter += 1
        self._notify_event.set()

    def notify_alive_after(self):
        """
        Notifies the watchdog that the Trio thread is alive after running a
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
                    print(
                        "Trio Watchdog has not received any notifications in "
                        "5 seconds, main thread is blocked!",
                        file=sys.stderr,
                    )
                    # faulthandler is not very useful to us, honestly
                    # faulthandler.dump_traceback(all_threads=True)
                    print("Printing the traceback of all threads:", file=sys.stderr)
                    self._print_all_threads()

    def _print_all_threads(self):
        # separated for indent reasons, damned 80 char limit
        for thread in threading.enumerate():
            print("Thread {} (most recent call last):".format(thread.name), file=sys.stderr)
            # scary internal function!
            traceback.print_stack(sys._current_frames()[thread.ident])

    def start(self):
        self._thread = threading.Thread(target=self._main_loop, name="<trio watchdog>", daemon=True)
        self._thread.start()

    def stop(self):
        self._stopped = True
        self._notify_event.set()
        self._thread.join()
