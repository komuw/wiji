import wiji
import threading


class AdderTask(wiji.task.Task):
    the_broker = wiji.broker.InMemoryBroker()
    queue_name = "AdderTaskQueue1"

    async def run(self, a, b):
        res = a + b
        print()
        print("AdderTask111:: ")
        print(threading.enumerate())
        print()
        return res


class SubTask(wiji.task.Task):
    the_broker = wiji.broker.InMemoryBroker()
    queue_name = "SubTask"

    async def run(self, a, b):
        res = a + b
        print()
        print("SubTask:: ")
        print(threading.enumerate())
        print()
        return res


# run cli as:
#   wiji-cli --app examples.my_app.MyAppInstance
MyAppInstance = wiji.app.App(task_classes=[AdderTask, SubTask])


subber = SubTask()
myAdderTask = AdderTask()
for i in range(0, 200):
    myAdderTask.synchronous_delay(67, i)
    subber.synchronous_delay(34, i)

if __name__ == "__main__":
    myAdderTask = AdderTask()
    myAdderTask.synchronous_delay(67, 887)
