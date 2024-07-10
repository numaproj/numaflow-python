from typing import AsyncIterable

from pynumaflow.batchmapper import (
    Message,
    Datum,
    BatchMapper,
    BatchMapAsyncServer,
    BatchResponses,
    BatchResponse,
)


class Flatmap(BatchMapper):
    """
    This is a class that inherits from the BatchMapper class.
    It implements a flatmap operation over a batch of input messages
    """

    async def handler(
        self,
        datums: AsyncIterable[Datum],
    ) -> BatchResponses:
        batch_responses = BatchResponses()
        async for datum in datums:
            val = datum.value
            _ = datum.event_time
            _ = datum.watermark
            strs = val.decode("utf-8").split(",")
            batch_response = BatchResponse.new_batch_response(datum.id)
            if len(strs) == 0:
                batch_response.append(Message.to_drop())
            else:
                for s in strs:
                    batch_response.append(Message(str.encode(s)))
            batch_responses.append(batch_response)

        return batch_responses


if __name__ == "__main__":
    """
    This example shows how to use the Flatmap mapper.
    We use a class as handler, but a function can be used as well.
    """
    grpc_server = BatchMapAsyncServer(Flatmap())
    grpc_server.start()
