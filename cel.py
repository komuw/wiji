import celery


"""
run as:
    python cel.py &; celery worker -A cel:celobj
      or
    celery worker -A cel:celobj --concurrency=1 --soft-time-limit=25 --loglevel=INFO --pool=komupool
"""
celobj = celery.Celery()


@celobj.task(name="sync_req")
def sync_req(url):
    import requests

    res = requests.get(url=url, timeout=90)
    print("res: ", res)


@celobj.task(name="Async_req", this_is_async_task=True)
async def Async_req(url):
    import asyncio
    import aiohttp

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            print("resp statsus: ", resp.status)
            res_text = await resp.text()
            print(res_text[:50])


@celobj.task(name="adder")
def adder(a, b):
    res = a + b
    print("adder: ", res)
    return res


@celobj.task(name="divider")
def divider(a):
    res = a / 3
    print("divider: ", res)
    return res


if __name__ == "__main__":
    # sync_req.delay(url="https://httpbin.org/delay/5")
    # print("!!! SYNC messages enqueued !!!")

    # # for _ in range(0, 10):
    # Async_req.delay(url="https://httpbin.org/delay/7")
    # print("!!! Async messages enqueued !!!")

    chain = adder.s(3, 7).set(queue="adder") | divider.s().set(queue="divider")
    chain()
    print("!!! chain enqueued !!!")
    """
    The above enques only ONE task, the adder task with a body payload like.
        [
        [
            3,
            7
        ],
        {},
        {
            "callbacks": null,
            "errbacks": null,
            "chain": [
                {
                    "task": "divider",
                    "args": [],
                    "kwargs": {},
                    "options": {
                    "queue": "divider",
                    "task_id": "05bd2f0f-de20-498b-bc96-75c8a42fcb99",
                    "reply_to": "93db68e0-9696-3f15-ac26-29e44f857c33"
                    },
                    "subtask_type": null,
                    "chord_size": null,
                    "immutable": false
                }
            ],
            "chord": null
        }
        ]
    """
