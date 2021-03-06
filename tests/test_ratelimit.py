import time
import asyncio
from unittest import TestCase, mock

import wiji


def AsyncMock(*args, **kwargs):
    """
    see: https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
    """
    m = mock.MagicMock(*args, **kwargs)

    async def mock_coro(*args, **kwargs):
        return m(*args, **kwargs)

    mock_coro.mock = m
    return mock_coro


class TestRateLimit(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_ratelimit.TestRateLimit.test_something
    """

    def setUp(self):
        self.logger = wiji.logger.SimpleLogger("myTestLogger")
        self.execution_rate = 1.00
        self.rateLimiter = wiji.ratelimiter.SimpleRateLimiter(execution_rate=self.execution_rate)

    def tearDown(self):
        pass

    @staticmethod
    def _run(coro):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_no_rlimit(self):
        with mock.patch("wiji.ratelimiter.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            for _ in range(0, int(self.execution_rate)):
                self._run(self.rateLimiter.limit())
            self.assertFalse(mock_sleep.mock.called)

    def test_token_exhaustion_causes_rlimit(self):
        with mock.patch("wiji.ratelimiter.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            for _ in range(0, int(self.execution_rate) * 2):
                self._run(self.rateLimiter.limit())
            self.assertTrue(mock_sleep.mock.called)
            self.assertEqual(mock_sleep.mock.call_args[0][0], self.rateLimiter.delay_for_tokens)

    def test_execution_rate(self):
        execution_rate = 3.00
        rLimiter = wiji.ratelimiter.SimpleRateLimiter(
            log_handler=self.logger, execution_rate=execution_rate
        )
        msgs_delivered = []
        now = time.monotonic()
        for _ in range(0, int(execution_rate) * 4):
            z = self._run(rLimiter.limit())
            msgs_delivered.append(z)

        then = time.monotonic()
        time_taken_to_deliver = then - now  # seconds
        total_msgs_delivered = len(msgs_delivered)
        effective_message_rate = total_msgs_delivered / time_taken_to_deliver
        self.assertAlmostEqual(effective_message_rate, execution_rate, 0)
