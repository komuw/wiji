import uuid
import datetime


eta = datetime.datetime.utcnow() + datetime.timedelta(seconds=task_options.eta)
protocol = {
    "version": 1,
    "task_id": str(uuid.uuid4()),
    "eta": eta.isoformat(),
    "retries": 0,
    "queue": "myQueue",
    "file_name": "cool.Task",
    "class_path": "Users.komuw.mystuff.xyzabc.cool.Task",
    "timelimit": 1800,
    "args": (33, "hello"),
    "kwargs": {"name": "komu"},
}
