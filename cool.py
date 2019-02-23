import asyncio
import aiohttp
import requests
import time


async def Async_http():
    url = "https://httpbin.org/delay/23"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            print()
            print("Async_http:")
            print("resp statsus: ", resp.status)
            res_text = await resp.text()
            print(res_text[:50])


def SYNC_http():
    print()
    print("SYNC_http:")
    url = "https://httpbin.org/delay/23"
    resp = requests.get(url)


class MyBase:
    pass


class Deriv(MyBase):
    pass


if __name__ == "__main__":

    async def async_main():
        xxx = Deriv()
        import pdb

        pdb.set_trace()
        ############### ASYNC ###############
        start_time_cpu = time.process_time()
        start_time_io = time.monotonic()

        await Async_http()
        end_time_cpu = time.process_time()
        end_time_io = time.monotonic()

        diff_cpu_time = end_time_cpu - start_time_cpu
        diff_io_time = end_time_io - start_time_io

        print()
        print("Async_http took:")
        print("diff_cpu_time: ", diff_cpu_time)
        print("diff_io_time: ", diff_io_time)
        #############################################

        ########### SYNC ###############
        start_time_cpu = time.process_time()
        start_time_io = time.monotonic()

        SYNC_http()
        end_time_cpu = time.process_time()
        end_time_io = time.monotonic()

        diff_cpu_time = end_time_cpu - start_time_cpu
        diff_io_time = end_time_io - start_time_io

        print()
        print("SYNC_http took:")
        print("diff_cpu_time: ", diff_cpu_time)
        print("diff_io_time: ", diff_io_time)
        #############################################

    asyncio.run(async_main(), debug=True)
