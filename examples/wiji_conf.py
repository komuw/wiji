import wiji

from examples.example_tasks import (
    print_task2,
    multiplier,
    divider,
    adder,
    http_task1,
    exception_task22,
    BLOCKING_task,
)

# run cli as:
#   wiji-cli --config examples.wiji_conf.yolo
yolo = wiji.conf.WijiConf(
    tasks=[print_task2, multiplier, divider, adder, http_task1, exception_task22]
)
