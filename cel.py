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


if __name__ == "__main__":
    sync_req.delay(url="https://httpbin.org/delay/5")
    print("!!! SYNC messages enqueued !!!")

    # for _ in range(0, 10):
    Async_req.delay(url="https://httpbin.org/delay/7")
    print("!!! Async messages enqueued !!!")

