# do not to pollute the global namespace.
# see: https://python-packaging.readthedocs.io/en/latest/testing.html
import json
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

    def test_eta_conversions(self):
        """
        test eta when calling `Task.delay` and `Worker.consume_tasks`
        """
        task_options = wiji.task.TaskOptions()
        proto = wiji.protocol.Protocol(version=1, task_options=task_options)
        _dequeued_item = proto.json()
        dequeued_item = json.loads(_dequeued_item)
        _task_options = dequeued_item["task_options"]
        task_eta = _task_options["eta"]
        self.assertEqual(task_eta, task_options.eta)
        self.assertEqual(
            wiji.protocol.Protocol._from_isoformat(task_eta),
            wiji.protocol.Protocol._from_isoformat(task_options.eta),
        )
