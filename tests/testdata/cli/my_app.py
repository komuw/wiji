import wiji


class AdderTask(wiji.task.Task):
    the_broker = wiji.broker.InMemoryBroker()
    queue_name = "AdderTaskQueue89"

    async def run(self, a, b):
        res = a + b
        return res


MyAppInstance = wiji.app.App(task_classes=[AdderTask])
