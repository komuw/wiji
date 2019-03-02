async def produce_tasks_continously(task, *args, **kwargs):
    while True:
        await task.delay(*args, **kwargs)
