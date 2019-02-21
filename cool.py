import asyncio


class AsyncIteratorExecutor:
    """
    Converts a regular iterator into an asynchronous
    iterator, by executing the iterator in a thread.
    """

    def __init__(self, iterator, loop=None, executor=None):
        self.__iterator = iterator
        self.__loop = loop or asyncio.get_event_loop()
        self.__executor = executor

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.__loop.run_in_executor(self.__executor, next, self.__iterator, self)
        if value is self:
            raise StopAsyncIteration
        return value


#########
class AsyncRange(object):
    def __init__(self, length):
        self.length = length
        self.i = 0

    async def __aiter__(self):
        return self

    async def __anext__(self):
        index = self.i
        self.i += 1
        if self.i <= self.length:
            return index
        else:
            raise StopAsyncIteration


async def foo():
    # AsyncRange(my_list)
    async for char in AsyncRange(iter([1, 2, 34, 5, 5])):
        print(char)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        # loop.run_until_complete(cat_file_async("/Users/komuw/mystuff/xyzabc/cool.py"))
        loop.run_until_complete(foo())
    finally:
        loop.close()
