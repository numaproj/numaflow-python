import threading
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterable

from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow.shared.server import exit_on_error
from pynumaflow.shared.synciter import SyncIterator
from pynumaflow.sourcetransformer import Datum
from pynumaflow.sourcetransformer._dtypes import SourceTransformCallable
from pynumaflow.proto.sourcetransformer import transform_pb2
from pynumaflow.proto.sourcetransformer import transform_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import (
    _LOGGER,
    STREAM_EOF,
    NUM_THREADS_DEFAULT,
    ERR_UDF_EXCEPTION_STRING,
)


def _create_read_handshake_response() -> transform_pb2.SourceTransformResponse:
    """
    Create a handshake response for the SourceTransform function.

    Returns:
    SourceTransformResponse: A SourceTransformResponse object indicating a successful handshake.
    """
    return transform_pb2.SourceTransformResponse(
        handshake=transform_pb2.Handshake(sot=True),
    )


class SourceTransformServicer(transform_pb2_grpc.SourceTransformServicer):
    """
    This class is used to create a new grpc SourceTransform servicer instance.
    It implements the SourceTransformServicer interface from the proto transform.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(self, handler: SourceTransformCallable, multiproc: bool = False):
        self.__transform_handler: SourceTransformCallable = handler
        # This indicates whether the grpc server attached is multiproc or not
        self.multiproc = multiproc
        # create a thread pool for executing UDF code
        self.executor = ThreadPoolExecutor(max_workers=NUM_THREADS_DEFAULT)

    def SourceTransformFn(
        self,
        request_iterator: Iterable[transform_pb2.SourceTransformRequest],
        context: NumaflowServicerContext,
    ) -> Iterable[transform_pb2.SourceTransformResponse]:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the generated transform_pb2_grpc.py file.
        """
        try:
            # The first message to be received should be a valid handshake
            req = next(request_iterator)
            # check if it is a valid handshake req
            if not (req.handshake and req.handshake.sot):
                raise Exception("SourceTransformFn: expected handshake message")
            yield _create_read_handshake_response()

            # result queue to stream messages from the user code back to the client
            result_queue = SyncIterator()

            # Reader thread to keep reading from the request iterator and schedule
            # execution for each of them
            reader_thread = threading.Thread(
                target=self._process_requests, args=(context, request_iterator, result_queue)
            )
            reader_thread.start()

            # Read the result queue and keep forwarding them upstream
            for res in result_queue.read_iterator():
                # if error handler accordingly
                if isinstance(res, BaseException):
                    # Terminate the current server process due to exception
                    exit_on_error(
                        context, f"{ERR_UDF_EXCEPTION_STRING}: {repr(res)}", parent=self.multiproc
                    )
                    return
                # return the result
                yield res

            # wait for the threads to clean-up
            reader_thread.join()
            self.executor.shutdown(cancel_futures=True)

        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            # Terminate the current server process due to exception
            exit_on_error(
                context, f"{ERR_UDF_EXCEPTION_STRING}: {repr(err)}", parent=self.multiproc
            )
            return

    def _process_requests(
        self,
        context: NumaflowServicerContext,
        request_iterator: Iterable[transform_pb2.SourceTransformRequest],
        result_queue: SyncIterator,
    ):
        try:
            # read through all incoming requests and submit to the
            # threadpool for invocation
            for request in request_iterator:
                _ = self.executor.submit(self._invoke_transformer, context, request, result_queue)
            # wait for all tasks to finish after all requests exhausted
            self.executor.shutdown(wait=True)
            # Indicate to the result queue that no more messages left to process
            result_queue.put(STREAM_EOF)
        except BaseException:
            _LOGGER.critical("SourceTransformFnError, re-raising the error", exc_info=True)

    def _invoke_transformer(
        self, context, request: transform_pb2.SourceTransformRequest, result_queue: SyncIterator
    ):
        try:
            d = Datum(
                keys=list(request.request.keys),
                value=request.request.value,
                event_time=request.request.event_time.ToDatetime(),
                watermark=request.request.watermark.ToDatetime(),
                headers=dict(request.request.headers),
            )
            responses = self.__transform_handler(list(request.request.keys), d)

            results = []
            for resp in responses:
                event_time_timestamp = _timestamp_pb2.Timestamp()
                event_time_timestamp.FromDatetime(dt=resp.event_time)
                results.append(
                    transform_pb2.SourceTransformResponse.Result(
                        keys=list(resp.keys),
                        value=resp.value,
                        tags=resp.tags,
                        event_time=event_time_timestamp,
                    )
                )
            result_queue.put(
                transform_pb2.SourceTransformResponse(results=results, id=request.request.id)
            )
        except BaseException as e:
            _LOGGER.critical("SourceTransform handler error", exc_info=True)
            result_queue.put(e)
            return

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> transform_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto transform_pb2_grpc.py file.
        """
        return transform_pb2.ReadyResponse(ready=True)
