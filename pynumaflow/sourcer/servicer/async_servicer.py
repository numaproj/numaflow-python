import asyncio
from collections.abc import AsyncIterable

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from pynumaflow.shared.asynciter import NonBlockingIterator

from pynumaflow.shared.server import handle_async_error
from pynumaflow.sourcer._dtypes import ReadRequest, Offset
from pynumaflow.sourcer._dtypes import AckRequest, SourceCallable
from pynumaflow.proto.sourcer import source_pb2
from pynumaflow.proto.sourcer import source_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER, STREAM_EOF, ERR_UDF_EXCEPTION_STRING


def _create_read_handshake_response():
    """Create a handshake response for the Read function."""
    return source_pb2.ReadResponse(
        status=source_pb2.ReadResponse.Status(
            eot=False, code=source_pb2.ReadResponse.Status.SUCCESS
        ),
        handshake=source_pb2.Handshake(sot=True),
    )


def _create_ack_handshake_response():
    """Create a handshake response for the Ack function."""
    return source_pb2.AckResponse(
        result=source_pb2.AckResponse.Result(success=_empty_pb2.Empty()),
        handshake=source_pb2.Handshake(sot=True),
    )


def _create_read_response(response):
    """Create a read response from the handler result."""
    event_time_timestamp = _timestamp_pb2.Timestamp()
    event_time_timestamp.FromDatetime(dt=response.event_time)
    result = source_pb2.ReadResponse.Result(
        payload=response.payload,
        keys=response.keys,
        offset=response.offset.as_dict,
        event_time=event_time_timestamp,
        headers=response.headers,
    )
    status = source_pb2.ReadResponse.Status(eot=False, code=source_pb2.ReadResponse.Status.SUCCESS)
    return source_pb2.ReadResponse(result=result, status=status)


def _create_eot_response():
    """Create an end-of-transmission response."""
    status = source_pb2.ReadResponse.Status(eot=True, code=source_pb2.ReadResponse.Status.SUCCESS)
    return source_pb2.ReadResponse(status=status)


def _create_ack_response():
    """Create an acknowledgement response."""
    return source_pb2.AckResponse(result=source_pb2.AckResponse.Result(success=_empty_pb2.Empty()))


class AsyncSourceServicer(source_pb2_grpc.SourceServicer):
    """
    This class is used to create a new grpc Source servicer instance.
    It implements the SourceServicer interface from the proto source.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(self, source_handler: SourceCallable):
        self.background_tasks = set()
        self.source_handler = source_handler
        self.__initialize_handlers()
        self.cleanup_coroutines = []

    def __initialize_handlers(self):
        """Initialize handler methods from the provided source handler."""
        self.__source_read_handler = self.source_handler.read_handler
        self.__source_ack_handler = self.source_handler.ack_handler
        self.__source_pending_handler = self.source_handler.pending_handler
        self.__source_partitions_handler = self.source_handler.partitions_handler

    async def ReadFn(
        self,
        request_iterator: AsyncIterable[source_pb2.ReadRequest],
        context: NumaflowServicerContext,
    ) -> AsyncIterable[source_pb2.ReadResponse]:
        """
        Handles the Read function, processing incoming requests and sending responses.
        """
        try:
            # The first message to be received should be a valid handshake
            req = await request_iterator.__anext__()
            # check if it is a valid handshake req
            if not (req.handshake and req.handshake.sot):
                raise Exception("ReadFn: expected handshake message")
            yield _create_read_handshake_response()

            # process the incoming read requests on the stream
            async for req in request_iterator:
                # create an iterator to be provided to the user function where the responses will
                # be streamed
                niter = NonBlockingIterator()
                riter = niter.read_iterator()
                task = asyncio.create_task(self.__invoke_read(req, niter))
                # Save a reference to the result of this function, to avoid a
                # task disappearing mid-execution.
                self.background_tasks.add(task)
                task.add_done_callback(self.clean_background)

                async for resp in riter:
                    if isinstance(resp, BaseException):
                        await handle_async_error(context, resp)
                        return

                    yield _create_read_response(resp)

                # ensure that the task has completed
                await task
                # send an eot to signal all messages have been processed.
                yield _create_eot_response()
        except BaseException as err:
            _LOGGER.critical("User-Defined Source ReadFn error", exc_info=True)
            await handle_async_error(context, err, ERR_UDF_EXCEPTION_STRING)

    async def __invoke_read(self, req, niter):
        """Invoke the read handler and manage the iterator."""
        try:
            await self.__source_read_handler(
                ReadRequest(
                    num_records=req.request.num_records, timeout_in_ms=req.request.timeout_in_ms
                ),
                niter,
            )
            # Put an EOF to the iterator to indicate that we have completed the processing the
            # requests from the user handler
            await niter.put(STREAM_EOF)
        except BaseException as err:
            _LOGGER.critical("User-Defined Source ReadFn error", exc_info=True)
            await niter.put(err)

    async def AckFn(
        self,
        request_iterator: AsyncIterable[source_pb2.AckRequest],
        context: NumaflowServicerContext,
    ) -> AsyncIterable[source_pb2.AckResponse]:
        """
        Handles the Ack function for user-defined source.
        """
        try:
            # The first message to be received should be a valid handshake
            req = await request_iterator.__anext__()
            # check if it is a valid handshake req
            if not (req.handshake and req.handshake.sot):
                raise Exception("AckFn: expected handshake message")
            yield _create_ack_handshake_response()

            # process the incoming Ack requests
            async for req in request_iterator:
                # proto repeated field(offsets) is of
                # type google._upb._message.RepeatedScalarContainer
                # we need to explicitly convert it to list
                offsets = [
                    Offset(offset.offset, offset.partition_id) for offset in req.request.offsets
                ]
                await self.__source_ack_handler(AckRequest(offsets=offsets))
                yield _create_ack_response()
        except BaseException as err:
            _LOGGER.critical("User-Defined Source AckFn error", exc_info=True)
            await handle_async_error(context, err, ERR_UDF_EXCEPTION_STRING)

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> source_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto source_pb2_grpc.py file.
        """
        return source_pb2.ReadyResponse(ready=True)

    async def PendingFn(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> source_pb2.PendingResponse:
        """
        PendingFn returns the number of pending records
        at the user defined source.
        """
        try:
            count = await self.__source_pending_handler()
        except BaseException as err:
            _LOGGER.critical("PendingFn Error", exc_info=True)
            await handle_async_error(context, err, ERR_UDF_EXCEPTION_STRING)
            return
        resp = source_pb2.PendingResponse.Result(count=count.count)
        return source_pb2.PendingResponse(result=resp)

    async def PartitionsFn(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> source_pb2.PartitionsResponse:
        """
        PartitionsFn returns the partitions of the user defined source.
        """
        try:
            partitions = await self.__source_partitions_handler()
        except BaseException as err:
            _LOGGER.critical("PartitionsFn Error", exc_info=True)
            await handle_async_error(context, err, ERR_UDF_EXCEPTION_STRING)
            return
        resp = source_pb2.PartitionsResponse.Result(partitions=partitions.partitions)
        return source_pb2.PartitionsResponse(result=resp)

    def clean_background(self, task):
        """
        Remove the task from the background tasks collection
        """
        self.background_tasks.remove(task)
