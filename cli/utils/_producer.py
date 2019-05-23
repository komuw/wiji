import asyncio

import wiji


async def produce_tasks_continously(task: wiji.task.Task, watchdog_duration: float) -> None:
    """
    This function continously produces `wiji.task.WatchDogTask`.
    since the `WatchDogTask` checks for blockage on the eventloop over `watchdog_duration` seconds;
    it makes sense to produce `WatchDogTask` at rate greater than `watchdog_duration`
    hence the sleep duration chosen in this func.
    """
    while True:
        await task.delay()
        await asyncio.sleep(watchdog_duration / 20.00)
