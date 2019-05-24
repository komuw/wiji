import wiji


class AdderTask(wiji.task.Task):
    the_broker = wiji.broker.InMemoryBroker()
    queue_name = "AdderTaskQueue1"

    async def run(self, a, b):
        res = a + b
        print()
        print("res:: ", res)
        print()
        return res


# run cli as:
#   wiji-cli --app examples.my_app.MyAppInstance
MyAppInstance = wiji.app.App(task_classes=[AdderTask])


if __name__ == "__main__":
    myAdderTask = AdderTask()
    myAdderTask.synchronous_delay(67, 887)
