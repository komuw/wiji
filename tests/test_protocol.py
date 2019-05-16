# do not to pollute the global namespace.
# see: https://python-packaging.readthedocs.io/en/latest/testing.html
import datetime
from unittest import TestCase

import wiji


class TestProtocol(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_protocol.TestProtocol.test_something
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_from_isoformat(self):
        dt = wiji.protocol.Protocol._from_isoformat(eta="2020-04-01T18:41:54.195638+00:00")
        self.assertEqual(
            dt, datetime.datetime(2020, 4, 1, 18, 41, 54, 195638, tzinfo=datetime.timezone.utc)
        )

        # this guards against: https://github.com/komuw/wiji/issues/61
        dt = wiji.protocol.Protocol._from_isoformat(eta="2019-05-15T14:06:43+00:00")
        self.assertEqual(
            dt, datetime.datetime(2019, 5, 15, 14, 6, 43, tzinfo=datetime.timezone.utc)
        )

    def test_to_isoformat(self):
        iso_fmt_str = wiji.protocol.Protocol._eta_to_isoformat(0.00)
        self.assertIsInstance(iso_fmt_str, str)

        iso_fmt_str = wiji.protocol.Protocol._eta_to_isoformat(3.41)
        self.assertIsInstance(iso_fmt_str, str)
