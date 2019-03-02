import os
import sys
from celery import concurrency

from celery.five import monotonic, reraise
from celery.exceptions import WorkerShutdown, WorkerTerminate
from billiard.exceptions import WorkerLostError
from billiard.einfo import ExceptionInfo


def apply_target(
    target,
    args=(),
    kwargs={},
    callback=None,
    accept_callback=None,
    pid=None,
    getpid=os.getpid,
    propagate=(),
    monotonic=monotonic,
    **_,
):
    """Apply function within pool context."""
    if accept_callback:
        accept_callback(pid or getpid(), monotonic())
    try:
        ret = target(*args, **kwargs)
    except propagate:
        raise
    except Exception:
        raise
    except (WorkerShutdown, WorkerTerminate):
        raise
    except BaseException as exc:
        try:
            reraise(WorkerLostError, WorkerLostError(repr(exc)), sys.exc_info()[2])
        except WorkerLostError:
            callback(ExceptionInfo())
    else:
        callback(ret)


class MyTaskPool(concurrency.base.BasePool):
    """
    Solo task pool (blocking, inline, fast).

    Taken from: https://github.com/celery/celery/blob/master/celery/concurrency/solo.py

    run as: 
      celery worker -A cel:c --concurrency=1 --soft-time-limit=25 --loglevel=INFO --pool=komupool
    """

    body_can_be_buffer = True

    def __init__(self, *args, **kwargs):
        super(MyTaskPool, self).__init__(*args, **kwargs)
        self.on_apply = apply_target  # or; concurrency.base.apply_target
        self.limit = 1

    def _get_info(self):
        return {
            "max-concurrency": 1,
            "processes": [os.getpid()],
            "max-tasks-per-child": None,
            "put-guarded-by-semaphore": True,
            "timeouts": (),
        }


Ala = MyTaskPool()
