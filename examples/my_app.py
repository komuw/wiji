import wiji


# BROKER = wiji.broker.InMemoryBroker()

from examples.redis_broker import ExampleRedisBroker


MY_BROKER = ExampleRedisBroker()


class AdderTask(wiji.task.Task):
    the_broker = MY_BROKER
    queue_name = "AdderTaskQueue1"

    async def run(self, a, b):
        res = a + b
        print()
        print("res:: ", res)
        print()
        return res


# run cli as:
#   wiji-cli --app examples.my_app.MyAppInstance
MyAppInstance = wiji.app.App(task_classes=[AdderTask], watchdog_duration=20.0)


if __name__ == "__main__":
    for i in range(0, 122):
        myAdderTask = AdderTask()
        myAdderTask.synchronous_delay(67, i)
