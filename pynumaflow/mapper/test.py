import asyncio
from abc import ABCMeta, abstractmethod

import aiorun


class MapperClass(metaclass=ABCMeta):
    """
    Provides an interface to write a Mapper
    which will be exposed over a Synchronous gRPC server.

    Args:

    """

    def __call__(self, *args, **kwargs):
        """
        Allow to call handler function directly if class instance is sent
        """
        return self.handler(*args, **kwargs)

    @abstractmethod
    def handler(self, key: str) -> int:
        """
        Write a handler function which implements the MapCallable interface.
        """
        pass


class Sync(MapperClass):
    def handler(self, key: str) -> int:
        print("Sync")
        return 1


class Async(MapperClass):
    async def handler(self, key: str) -> int:
        await self.hey(key)

    async def hey(self, key):
        print(key)
        await asyncio.sleep(10)
        return 1
        # return await self.handler(


if __name__ == "__main__":
    hello = Sync()
    print(hello.handler("hello"))
    hello = Async()
    aiorun.run(hello.handler("hello"))
    hello2 = Async()
    aiorun.run(hello2.handler("hello2"))
