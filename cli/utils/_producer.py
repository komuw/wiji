import asyncio

import wiji


async def produce_tasks_continously(task: wiji.task.Task, watchdog_duration: float) -> None:
    while True:
        await task.delay()
        await asyncio.sleep(watchdog_duration / 8)
