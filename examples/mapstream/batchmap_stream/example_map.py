import os
import time
import json
from collections.abc import AsyncIterable
from pynumaflow.mapstreamer import Message, Datum, MapStreamAsyncServer, MapStreamer

SLEEP_TIME = int(os.environ.get("SLEEP_TIME_SEC", "1"))


class MapperStreamer(MapStreamer):
    async def handler(self, keys: list[str], datum: Datum) -> AsyncIterable[Message]:
        pass

    async def handler_stream(self, datum: AsyncIterable[Datum]) -> AsyncIterable[Message]:
        """
        A handler to iterate over each item in stream and will output message for each item.
        For example, indicates even, odd, or DROP if 0.

        This will sleep a very short time to simulate longer processing so that we can see
        messages actually backing up and getting fetched and processed in batches
        """
        all_grouped = []

        # Treat each message individually, because we can

        print(f"Simulate doing work for {SLEEP_TIME} sec")
        time.sleep(SLEEP_TIME)
        async for msg in datum:
            parsed = json.loads(msg.value.decode())
            val = hash(parsed["Data"]["padding"])

            as_str = str(val)
            all_grouped.append(val)
            print(f"Computed message value = {as_str}")

            last_int = int(as_str[-1])
            if last_int == 0:
                print(f"Drop {as_str}")
                yield Message.to_drop()
                continue

            if last_int % 2 == 0:
                output_keys = ["even"]
                output_tags = ["even-tag"]
            else:
                output_keys = ["odd"]
                output_tags = ["odd-tag"]
            yield Message(value=as_str.encode("utf-8"), keys=output_keys, tags=output_tags)

        # Show that we can do a messages separate from each individual one.
        # This demonstrates 'grouping' messages into fewer, but larger ,messages
        grouped_val = json.dumps(all_grouped).encode("utf-8")
        yield Message(value=grouped_val, tags=["grouped"])


if __name__ == "__main__":
    # NOTE: stream handler does currently support function-only handler
    handler = MapperStreamer()
    grpc_server = MapStreamAsyncServer(handler)
    grpc_server.start()
