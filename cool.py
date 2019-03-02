import asyncio


class Aiter:
    def __init__(self, iterable):
        self.iter_ = iter(iterable)

    async def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0)
        try:
            object = next(self.iter_)
        except StopIteration:
            raise StopAsyncIteration  # :-) PEP492 - "To stop iteration __anext__ must raise a StopAsyncIteration exception"

        return object


class AsyncIterFromTasks:
    def __init__(self, tasks):
        self.not_done = list(tasks)
        self.done = set()

    async def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.done and self.not_done:
            self.done, self.not_done = await asyncio.wait(
                self.not_done, return_when=asyncio.FIRST_COMPLETED
            )
        if not self.done:
            assert not self.not_done
            raise StopAsyncIteration()
        return self.done.pop()


class AsyncYield:
    def __init__(self, value):
        self.value = value

    def __await__(self):
        yield self.value


class AsyncIter:
    def __init__(self, ll):
        self.i = 0
        self.ll = ll
        self.len_ = len(self.ll)
        self.popper = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.i += 1
        if self.i > self.len_:
            raise StopAsyncIteration

        val = self.ll[self.popper]
        self.popper += 1

        return await asyncio.sleep(delay=0.05, result=val)


if __name__ == "__main__":

    async def async_main():
        async for i in AsyncIter([1, 2, 3]):
            print(i)

    asyncio.run(async_main(), debug=False)
