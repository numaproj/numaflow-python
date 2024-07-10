import asyncio
from collections.abc import AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.batchmapper import Datum
from pynumaflow.batchmapper._dtypes import BatchMapCallable
from pynumaflow.proto.batchmapper import batchmap_pb2, batchmap_pb2_grpc
from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow.shared.server import exit_on_error
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER, STREAM_EOF


async def datum_generator(
    request_iterator: AsyncIterable[batchmap_pb2.BatchMapRequest],
) -> AsyncIterable[Datum]:
    """
    This function is used to create an async generator
    from the gRPC request iterator.
    It yields a ReduceRequest instance for each request received which is then
    forwarded to the task manager.
    """
    async for d in request_iterator:
        request = Datum(
            keys=d.keys,
            value=d.value,
            event_time=d.event_time.ToDatetime(),
            watermark=d.watermark.ToDatetime(),
            headers=dict(d.headers),
            id=d.id,
        )
        yield request


class AsyncBatchMapServicer(batchmap_pb2_grpc.BatchMapServicer):
    """
    This class is used to create a new grpc Map Stream Servicer instance.
    It implements the BatchMapServicer interface from the proto
    batchmap_pb2_grpc.py file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: BatchMapCallable,
    ):
        self.background_tasks = set()
        self.__batch_map_handler: BatchMapCallable = handler

    async def BatchMapFn(
        self,
        request_iterator: AsyncIterable[batchmap_pb2.BatchMapRequest],
        context: NumaflowServicerContext,
    ) -> batchmap_pb2.BatchMapResponse:
        """
        Applies a batch map function to a datum stream in streaming mode.
        The pascal case function name comes from the proto batchmap_pb2_grpc.py file.
        """
        # Create an async iterator from the request iterator
        datum_iterator = datum_generator(request_iterator=request_iterator)

        try:
            # invoke the UDF call for batch map
            responses, request_counter = await self.invoke_batch_map(datum_iterator)

            # If the number of responses received does not align with the request batch size,
            # we will not be able to process the data correctly.
            # This should be marked as an error and raised to the user.
            if len(responses) != request_counter:
                err_msg = "batchMapFn: mismatch between length of batch requests and responses"
                raise Exception(err_msg)

            # iterate over the responses received and covert to the required proto format
            for batch_response in responses:
                single_req_resp = []
                for msg in batch_response.messages:
                    single_req_resp.append(
                        batchmap_pb2.BatchMapResponse.Result(
                            keys=msg.keys, value=msg.value, tags=msg.tags
                        )
                    )

                yield batchmap_pb2.BatchMapResponse(id=batch_response.id(), results=single_req_resp)

        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            await asyncio.gather(
                context.abort(grpc.StatusCode.UNKNOWN, details=repr(err)), return_exceptions=True
            )
            exit_on_error(context, repr(err))
            return

    async def invoke_batch_map(self, datum_iterator: AsyncIterable[Datum]):
        """
        # iterate over the incoming requests, and keep sending to the user code
        # once all messages have been sent, we wait for the responses
        """
        # create a message queue to send to the user code
        niter = NonBlockingIterator()
        riter = niter.read_iterator()
        # create a task for invoking the UDF handler
        task = asyncio.create_task(self.__batch_map_handler(riter))
        # Save a reference to the result of this function, to avoid a
        # task disappearing mid-execution.
        self.background_tasks.add(task)
        task.add_done_callback(lambda t: self.background_tasks.remove(t))

        req_count = 0
        # start streaming the messages to the UDF code, and increment the request counter
        async for datum in datum_iterator:
            await niter.put(datum)
            req_count += 1

        # once all messages have been exhausted, send an EOF to indicate stop of execution
        await niter.put(STREAM_EOF)

        # wait for the responses
        await task

        return task.result(), req_count

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> batchmap_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto batchmap_pb2_grpc.py file.
        """
        return batchmap_pb2.ReadyResponse(ready=True)
