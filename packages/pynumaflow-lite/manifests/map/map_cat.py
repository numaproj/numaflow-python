import asyncio
from pynumaflow_lite.mapper import Mapper, Message, Datum, MapAsyncServer


class SimpleCat(Mapper):
    async def handler(self, datum: Datum) -> list[Message]:
        if datum.value == b"bad world":
            return [Message.to_drop()]
        print(f"Received {datum=}")
        return [Message(datum.value, keys=datum.keys)]


async def main() -> None:
    print("Starting map server")
    # `serve` returns when SIGINT or SIGTERM arrives.
    await MapAsyncServer(SimpleCat()).serve()
    print("Map server stopped")


if __name__ == "__main__":
    asyncio.run(main())
