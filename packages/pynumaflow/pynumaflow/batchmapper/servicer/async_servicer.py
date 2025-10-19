import asyncio
from collections.abc import AsyncIterable

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.batchmapper import Datum
from pynumaflow.batchmapper._dtypes import BatchMapCallable, BatchMapError
from pynumaflow.proto.mapper import map_pb2, map_pb2_grpc
from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow.shared.server import handle_async_error
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER, STREAM_EOF, ERR_UDF_EXCEPTION_STRING


class AsyncBatchMapServicer(map_pb2_grpc.MapServicer):
    """
    This class is used to create a new grpc Batch Map Servicer instance.
    It implements the MapServicer interface from the proto
    map_pb2_grpc.py file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: BatchMapCallable,
    ):
        self.background_tasks = set()
        self.__batch_map_handler: BatchMapCallable = handler

    async def MapFn(
        self,
        request_iterator: AsyncIterable[map_pb2.MapRequest],
        context: NumaflowServicerContext,
    ) -> AsyncIterable[map_pb2.MapResponse]:
        """
        Applies a batch map function to a MapRequest stream in a batching mode.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        try:
            # The first message to be received should be a valid handshake
            req = await request_iterator.__anext__()
            # check if it is a valid handshake req
            if not (req.handshake and req.handshake.sot):
                raise BatchMapError("BatchMapFn: expected handshake as the first message")
            yield map_pb2.MapResponse(handshake=map_pb2.Handshake(sot=True))

            # cur_task is used to track the task (coroutine) processing
            # the current batch of messages.
            cur_task = None
            # iterate of the incoming messages ot the sink
            async for d in request_iterator:
                # if we do not have any active task currently processing the batch
                # we need to create one and call the User function for processing the same.
                if cur_task is None:
                    req_queue = NonBlockingIterator()
                    cur_task = asyncio.create_task(
                        self.__batch_map_handler(req_queue.read_iterator())
                    )
                    self.background_tasks.add(cur_task)
                    cur_task.add_done_callback(self.background_tasks.discard)
                # when we have end of transmission message, we need to stop the processing the
                # current batch and wait for the next batch of messages.
                # We will also wait for the current task to finish processing the current batch.
                # We mark the current task as None to indicate that we are
                # ready to process the next batch.
                if d.status and d.status.eot:
                    await req_queue.put(STREAM_EOF)
                    await cur_task
                    ret = cur_task.result()

                    # iterate over the responses received and covert to the required proto format
                    for batch_response in ret:
                        single_req_resp = []
                        for msg in batch_response.messages:
                            single_req_resp.append(
                                map_pb2.MapResponse.Result(
                                    keys=msg.keys, value=msg.value, tags=msg.tags
                                )
                            )
                        # send the response for a given ID back to the stream
                        yield map_pb2.MapResponse(id=batch_response.id, results=single_req_resp)

                    # send EOT after each finishing Batch responses
                    yield map_pb2.MapResponse(status=map_pb2.TransmissionStatus(eot=True))
                    cur_task = None
                    continue

                # if we have a valid message, we will add it to the request queue for processing.
                datum = Datum(
                    keys=list(d.request.keys),
                    value=d.request.value,
                    event_time=d.request.event_time.ToDatetime(),
                    watermark=d.request.watermark.ToDatetime(),
                    headers=dict(d.request.headers),
                    id=d.id,
                )
                await req_queue.put(datum)

        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            await handle_async_error(context, err, ERR_UDF_EXCEPTION_STRING)
            return

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto batchmap_pb2_grpc.py file.
        """
        return map_pb2.ReadyResponse(ready=True)
