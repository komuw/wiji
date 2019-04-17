import wiji
import asyncio


class AdderTask(wiji.task.Task):
    unique_name = "myAdderUniqueName"

    async def run(self, a, b):
        res = a + b
        print()
        print("res:: ", res)
        print()
        return res


# run cli as:
#   wiji-cli --config examples.my_app.MyAppInstance
MyAppInstance = wiji.app.App(task_classes=[AdderTask])


BROKER = wiji.broker.InMemoryBroker()
myAdderTask = AdderTask(the_broker=BROKER, queue_name="AdderTaskQueue1")
myAdderTask.synchronous_delay(67, 887)
