import asyncio
from collections.abc import AsyncIterator

from google.protobuf import empty_pb2 as _empty_pb2
from pynumaflow.shared.asynciter import NonBlockingIterator

from pynumaflow.shared.server import update_context_err
from pynumaflow.sinker._dtypes import Datum, SinkAsyncCallable
from pynumaflow.proto.sinker import sink_pb2_grpc, sink_pb2
from pynumaflow.sinker.servicer.utils import (
    datum_from_sink_req,
    _create_read_handshake_response,
    build_sink_resp_results,
)
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER, STREAM_EOF, ERR_UDF_EXCEPTION_STRING


class AsyncSinkServicer(sink_pb2_grpc.SinkServicer):
    """
    This class is used to create a new grpc Sink servicer instance.
    It implements the SinkServicer interface from the proto sink.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: SinkAsyncCallable,
    ):
        self.background_tasks = set()
        self.__sink_handler: SinkAsyncCallable = handler
        self.cleanup_coroutines = []
        self._shutdown_event: asyncio.Event | None = None
        self._error: BaseException | None = None

    def set_shutdown_event(self, event: asyncio.Event):
        """Wire up the shutdown event created by the server's aexec() coroutine."""
        self._shutdown_event = event

    async def SinkFn(
        self,
        request_iterator: AsyncIterator[sink_pb2.SinkRequest],
        context: NumaflowServicerContext,
    ) -> sink_pb2.SinkResponse:
        """
        Applies a sink function to a list of datum elements.
        The pascal case function name comes from the proto sink_pb2_grpc.py file.
        """
        try:
            # The first message to be received should be a valid handshake
            req = await request_iterator.__anext__()
            # check if it is a valid handshake req
            if not (req.handshake and req.handshake.sot):
                raise Exception("ReadFn: expected handshake message")
            yield _create_read_handshake_response()

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
                        self.__invoke_sink(req_queue.read_iterator(), context)
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
                    yield sink_pb2.SinkResponse(results=ret)
                    # send EOT after each finishing sink responses
                    yield sink_pb2.SinkResponse(status=sink_pb2.TransmissionStatus(eot=True))
                    cur_task = None
                    continue

                # if we have a valid message, we will add it to the request queue for processing.
                datum = datum_from_sink_req(d)
                await req_queue.put(datum)
        except BaseException as err:
            err_msg = f"UDSinkError, {ERR_UDF_EXCEPTION_STRING}: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            update_context_err(context, err, err_msg)
            # Store the error and signal the server to shut down gracefully
            # instead of killing the process via SIGKILL.
            self._error = err
            if self._shutdown_event:
                self._shutdown_event.set()
            return

    async def __invoke_sink(
        self, request_queue: AsyncIterator[Datum], context: NumaflowServicerContext
    ):
        try:
            # invoke the user function with the request queue
            rspns = await self.__sink_handler(request_queue)
            return build_sink_resp_results(rspns)
        except BaseException as err:
            err_msg = f"UDSinkError: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            raise err

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> sink_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto sink_pb2_grpc.py file.
        """
        return sink_pb2.ReadyResponse(ready=True)
