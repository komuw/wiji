import sys
import time
import asyncio
import logging
from unittest import TestCase, mock

import wiji


logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.CRITICAL)


def AsyncMock(*args, **kwargs):
    """
    see: https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
    """
    m = mock.MagicMock(*args, **kwargs)

    async def mock_coro(*args, **kwargs):
        return m(*args, **kwargs)

    mock_coro.mock = m
    return mock_coro


class TestWatchdog(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_watchdog.TestWatchdog.test_something
    """

    def setUp(self):
        # self.watchdog = wiji.watchdog.BlockingWatchdog(
        #     watchdog_duration=self.watchdog_duration, task_name=self.the_task.task_name
        # )
        pass

    def tearDown(self):
        pass

    @staticmethod
    def _run(coro):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_bad_instantiation(self):
        def create_watchdog():
            wiji.watchdog.BlockingWatchdog(watchdog_duration=1, task_name="task_name")

        self.assertRaises(ValueError, create_watchdog)
        with self.assertRaises(ValueError) as raised_exception:
            create_watchdog()
        self.assertIn(
            "watchdog_duration` should be of type:: `float` You entered: <class 'int'>",
            str(raised_exception.exception),
        )

    def test_breach(self):
        with mock.patch(
            "wiji.watchdog.BlockingWatchdog._save_stack_trace"
        ) as mock_save_stack_trace:
            mock_save_stack_trace.return_value = [
                {
                    "thread_name": "MainThread",
                    "thread_stack_trace": ["File some/some.py, line 269\n", "MOCK_stack_trace"],
                }
            ]
            watchdog_duration = 0.2
            dog = wiji.watchdog.BlockingWatchdog(
                watchdog_duration=watchdog_duration, task_name="task_name"
            )
            # start watchdog
            dog.start()
            dog.notify_alive_before()
            # carry out long blocking task
            time.sleep(watchdog_duration * 2)
            dog.notify_alive_after()
            dog.stop()
            self.assertTrue(mock_save_stack_trace.called)

    def test_no_breach(self):
        with mock.patch(
            "wiji.watchdog.BlockingWatchdog._save_stack_trace"
        ) as mock_save_stack_trace:
            mock_save_stack_trace.return_value = [
                {
                    "thread_name": "MainThread",
                    "thread_stack_trace": ["File some/some.py, line 269\n", "MOCK_stack_trace"],
                }
            ]
            watchdog_duration = 0.2
            dog = wiji.watchdog.BlockingWatchdog(
                watchdog_duration=watchdog_duration, task_name="task_name"
            )
            # start watchdog
            dog.start()
            dog.notify_alive_before()
            # carry out long blocking task
            time.sleep(watchdog_duration / 2)
            dog.notify_alive_after()
            dog.stop()
            self.assertFalse(mock_save_stack_trace.called)

    def test_non_blocking_no_breach(self):
        with mock.patch(
            "wiji.watchdog.BlockingWatchdog._save_stack_trace"
        ) as mock_save_stack_trace:
            mock_save_stack_trace.return_value = [
                {
                    "thread_name": "MainThread",
                    "thread_stack_trace": ["File some/some.py, line 269\n", "MOCK_stack_trace"],
                }
            ]
            watchdog_duration = 0.2
            dog = wiji.watchdog.BlockingWatchdog(
                watchdog_duration=watchdog_duration, task_name="task_name"
            )
            # start watchdog
            dog.start()
            dog.notify_alive_before()
            # carry out non-blocking task
            print("non-blocking task")
            dog.notify_alive_after()
            dog.stop()
            self.assertFalse(mock_save_stack_trace.called)

    def test_breach_logs_trace(self):
        with mock.patch(
            "wiji.watchdog.BlockingWatchdog._save_stack_trace"
        ) as mock_save_stack_trace, mock.patch(
            "wiji.watchdog.logger.SimpleLogger.log"
        ) as mock_logging:
            MOCK_stack_trace = "MOCK_stack_trace"
            mock_save_stack_trace.return_value = [
                {
                    "thread_name": "MainThread",
                    "thread_stack_trace": ["File some/some.py, line 269\n", MOCK_stack_trace],
                }
            ]
            mock_logging.return_value = None
            watchdog_duration = 0.2
            dog = wiji.watchdog.BlockingWatchdog(
                watchdog_duration=watchdog_duration, task_name="task_name"
            )
            # start watchdog
            dog.start()
            dog.notify_alive_before()
            # carry out long blocking task
            time.sleep(watchdog_duration * 2)
            dog.notify_alive_after()
            dog.stop()

            self.assertTrue(mock_save_stack_trace.called)
            self.assertTrue(mock_logging.called)
            self.assertIn(
                MOCK_stack_trace,
                mock_logging.call_args[0][1]["stack_trace"][0]["thread_stack_trace"],
            )
