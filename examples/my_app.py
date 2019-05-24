import wiji

import logging

import logging

# logging.getLogger("wiji").setLevel(logging.CRITICAL)


import logging

loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]

# import pdb

# pdb.set_trace()
logging.getLogger("wiji.task").setLevel(logging.CRITICAL)
# logging.getLogger("wiji")
# logging.getLogger("wiji")

print()
print("loggers:")
print(loggers)
print()


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

myAdderTask = AdderTask()
for i in range(0, 10):
    myAdderTask.synchronous_delay(67, i)

if __name__ == "__main__":
    myAdderTask = AdderTask()
    myAdderTask.synchronous_delay(67, 887)
