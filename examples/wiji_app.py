import wiji

from examples.example_tasks import (
    BlockingDiskIOTask,
    BlockinHttpTask,
    AsyncHttpTask,
    PrintTask,
    AdderTask,
    DividerTask,
    MultiplierTask,
    ExceptionTask,
)

# run cli as:
#   wiji-cli --app examples.wiji_app.yolo
yolo = wiji.app.App(
    task_classes=[
        BlockingDiskIOTask,
        BlockinHttpTask,
        AsyncHttpTask,
        PrintTask,
        AdderTask,
        DividerTask,
        MultiplierTask,
        ExceptionTask,
    ]
)
