import os
import json
import base64
from collections.abc import AsyncIterable
from pynumaflow.batchmapper import (
    Message,
    Messages,
    Datum,
    BatchMapper,
    BatchMapServer,
    BatchResponses,
)


class MapperStreamer(BatchMapper):
    async def handler(self, datum: AsyncIterable[Datum]) -> AsyncIterable[BatchResponses]:
        """
        A handler to demonstrate batching providing standard per-message actions as
        well as a grouping of all messages together - a value add of batch vs standard map
        """
        all_grouped = []

        # Retrieve and gather
        async for msg in datum:
            parsed = json.loads(msg.value.decode())
            val = hash(parsed["Data"]["padding"])
            base64.b64decode(parsed["Data"]["padding"])

            all_grouped.append((msg.id, val))

        for idx, id_and_val in enumerate(all_grouped):
            msg_id, val = id_and_val
            as_str = str(val)
            last_int = int(as_str[-1])
            if last_int == 0:
                yield BatchResponses.to_drop(msg_id)
                continue

            if last_int % 2 == 0:
                output_keys = ["even"]
                output_tags = ["even-tag"]
            else:
                output_keys = ["odd"]
                output_tags = ["odd-tag"]

            msgs = Messages(
                Message(value=as_str.encode("utf-8"), keys=output_keys, tags=output_tags)
            )
            # A final step to demonstrate 'grouping' values
            # Associate it with final message ID because that's what's left
            if idx == len(all_grouped) - 1:
                msgs.append(
                    Message(
                        value=json.dumps([x[1] for x in all_grouped]).encode("utf-8"),
                        keys=[],
                        tags=["grouped"],
                    )
                )
            response = BatchResponses(msg_id, msgs)
            yield response


if __name__ == "__main__":
    handler = MapperStreamer()
    grpc_server = BatchMapServer(handler)
    grpc_server.start()
