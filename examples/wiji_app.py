import wiji

from examples.example_tasks import (
    BlockingDiskIOTask,
    BlockinHttpTask,
    HttpTask,
    PrintTask,
    AdderTask,
    DividerTask,
    MultiplierTask,
    ExceptionTask,
)

# run cli as:
#   wiji-cli --config examples.wiji_app.yolo
yolo = wiji.app.App(
    task_classes=[
        BlockingDiskIOTask,
        BlockinHttpTask,
        HttpTask,
        PrintTask,
        AdderTask,
        DividerTask,
        MultiplierTask,
        ExceptionTask,
    ]
)
