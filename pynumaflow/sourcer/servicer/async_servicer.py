import asyncio
from collections.abc import AsyncIterable

import grpc
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from pynumaflow.shared.asynciter import NonBlockingIterator

from pynumaflow.shared.server import exit_on_error, handle_error
from pynumaflow.sourcer._dtypes import ReadRequest
from pynumaflow.sourcer._dtypes import AckRequest, SourceCallable
from pynumaflow.proto.sourcer import source_pb2
from pynumaflow.proto.sourcer import source_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER, STREAM_EOF


# async def read_datum_generator(
#         request_iterator: AsyncIterable[source_pb2.ReadRequest],
# ) -> AsyncIterable[ReadRequest]:
#     """
#     This function is used to create an async generator
#     from the gRPC request iterator.
#     It yields a Datum instance for each request received which is then
#     forwarded to the UDF.
#     """
#     async for d in request_iterator:
#         _LOGGER.info("d %s", d)
#         request = ReadRequest(
#             num_records=d.request.num_records,
#             timeout_in_ms=d.request.timeout_in_ms,
#         )
#         yield request


class AsyncSourceServicer(source_pb2_grpc.SourceServicer):
    """
    This class is used to create a new grpc Source servicer instance.
    It implements the SourceServicer interface from the proto source.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(self, source_handler: SourceCallable):
        self.background_tasks = set()
        self.source_handler = source_handler
        self.__source_read_handler = source_handler.read_handler
        self.__source_ack_handler = source_handler.ack_handler
        self.__source_pending_handler = source_handler.pending_handler
        self.__source_partitions_handler = source_handler.partitions_handler
        self.cleanup_coroutines = []

    async def ReadFn(
        self,
        request_iterator: AsyncIterable[source_pb2.ReadRequest],
        context: NumaflowServicerContext,
    ) -> source_pb2.ReadResponse:
        """
        Applies a Read function and returns a stream of datum responses.
        The pascal case function name comes from the proto source_pb2_grpc.py file.
        """
        try:
            need_handshake = True
            # Create an async iterator from the request iterator
            async for req in request_iterator:
                if need_handshake:
                    handshake_req = req
                    if not handshake_req.handshake or not handshake_req.handshake.sot:
                        err_msg = "ReadFn: expected handshake message"
                        raise Exception(err_msg)

                    yield source_pb2.ReadResponse(
                        status=source_pb2.ReadResponse.Status(
                            eot=False, code=source_pb2.ReadResponse.Status.SUCCESS
                        ),
                        handshake=source_pb2.Handshake(sot=True),
                    )
                    need_handshake = False
                    continue

                niter = NonBlockingIterator()
                riter = niter.read_iterator()
                task = asyncio.create_task(self.invoke_read(req, niter))
                # Save a reference to the result of this function, to avoid a
                # task disappearing mid-execution.
                self.background_tasks.add(task)
                task.add_done_callback(self.clean_background)

                async for resp in riter:
                    if isinstance(resp, BaseException):
                        handle_error(context, resp)
                        await asyncio.gather(
                            context.abort(grpc.StatusCode.UNKNOWN, details=repr(resp)),
                            return_exceptions=True,
                        )
                        exit_on_error(
                            err=repr(resp), parent=False, context=context, update_context=False
                        )
                        return
                    event_time_timestamp = _timestamp_pb2.Timestamp()
                    event_time_timestamp.FromDatetime(dt=resp.event_time)
                    result = source_pb2.ReadResponse.Result(
                        payload=resp.payload,
                        keys=resp.keys,
                        offset=resp.offset.as_dict,
                        event_time=event_time_timestamp,
                        headers=resp.headers,
                    )
                    status = source_pb2.ReadResponse.Status(
                        eot=False,
                        code=source_pb2.ReadResponse.Status.SUCCESS,
                    )
                    yield source_pb2.ReadResponse(result=result, status=status)

                await task
                status = source_pb2.ReadResponse.Status(
                    eot=True,
                    code=source_pb2.ReadResponse.Status.SUCCESS,
                )
                yield source_pb2.ReadResponse(status=status)
        except BaseException as err:
            _LOGGER.critical("User-Defined Source ReadFn error ", exc_info=True)
            exit_on_error(context, str(err))

    async def invoke_read(self, req, niter):
        # TODO(source-stream): check with this timeout
        try:
            await self.__source_read_handler(
                ReadRequest(
                    num_records=req.request.num_records, timeout_in_ms=req.request.timeout_in_ms
                ),
                niter,
            )
            await niter.put(STREAM_EOF)
        except BaseException as err:
            _LOGGER.critical("User-Defined Source ReadFn error ", exc_info=True)
            await niter.put(err)

    async def AckFn(
        self,
        request_iterator: AsyncIterable[source_pb2.AckRequest],
        context: NumaflowServicerContext,
    ) -> source_pb2.AckResponse:
        """
        Applies an Ack function in User Defined Source
        """
        # proto repeated field(offsets) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list

        try:
            need_handshake = True
            async for req in request_iterator:
                if need_handshake:
                    handshake_req = req
                    if not handshake_req.handshake or not handshake_req.handshake.sot:
                        err_msg = "AckFn: expected handshake message"
                        raise Exception(err_msg)

                    yield source_pb2.AckResponse(
                        result=source_pb2.AckResponse.Result(success=_empty_pb2.Empty()),
                        handshake=source_pb2.Handshake(sot=True),
                    )
                    need_handshake = False
                    continue

                await self.__source_ack_handler(AckRequest(req.request.offset))
                yield source_pb2.AckResponse(
                    result=source_pb2.AckResponse.Result(success=_empty_pb2.Empty())
                )
        except BaseException as err:
            _LOGGER.critical("User-Defined Source AckFn error ", exc_info=True)
            exit_on_error(context, repr(err))

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
            exit_on_error(context, repr(err))
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
            exit_on_error(context, repr(err))
            return
        resp = source_pb2.PartitionsResponse.Result(partitions=partitions.partitions)
        return source_pb2.PartitionsResponse(result=resp)

    def clean_background(self, task):
        """
        Remove the task from the background tasks collection
        """
        self.background_tasks.remove(task)
