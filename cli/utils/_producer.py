import asyncio

import wiji


async def produce_tasks_continously(task: wiji.task.Task) -> None:
    while True:
        await task.delay()
        await asyncio.sleep(0.00000000001)
