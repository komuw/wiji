import typing
import asyncio

import wiji


async def produce_tasks_continously(
    task: wiji.task.Task, *args: typing.Any, **kwargs: typing.Any
) -> None:
    while True:
        await task.delay(*args, **kwargs)
        await asyncio.sleep(1 / 117)
