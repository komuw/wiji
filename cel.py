import celery


"""
run as:
    python cel.py &; celery worker -A cel:c
"""
c = celery.Celery()


@c.task(name="hello")
def hello(name):
    print("hello: ", name)


if __name__ == "__main__":
    # for _ in range(0, 10):
    #     hello.apply_async(args=("delayed",), countdown=300)
    hello.delay(name="komu")
    print("!!! messages enqueued !!!")
