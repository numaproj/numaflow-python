import asyncio
from collections.abc import AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.batchmapper import Datum
from pynumaflow.batchmapper._dtypes import BatchMapCallable
from pynumaflow.proto.batchmapper import batchmap_pb2, batchmap_pb2_grpc
from pynumaflow.shared.server import exit_on_error
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER


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

        # iterate over the incoming requests, and keep appending to a request list
        # As the BatchMap interface expects a list of request elements
        # we read all the requests coming on the stream and keep appending them together
        # and then finally send the array for processing once all the messages on the stream
        # have been read.
        input_requests = []
        try:
            async for request in datum_iterator:
                input_requests.append(request)

            # invoke the UDF call for batch map
            responses = await self.__batch_map_handler(input_requests)

            # If the number of responses received does not align with the request batch size,
            # we will not be able to process the data correctly.
            # This should be marked as an error and raised to the user.
            if len(responses) != len(input_requests):
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

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> batchmap_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto batchmap_pb2_grpc.py file.
        """
        return batchmap_pb2.ReadyResponse(ready=True)
