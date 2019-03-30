import wiji


class AdderTask(wiji.task.Task):
    async def run(self, a, b):
        res = a + b
        return res


BROKER = wiji.broker.InMemoryBroker()
myAdderTask = AdderTask(the_broker=BROKER, queue_name="AdderTaskQueue1")

MyAppInstance = wiji.app.App(tasks=[myAdderTask])
