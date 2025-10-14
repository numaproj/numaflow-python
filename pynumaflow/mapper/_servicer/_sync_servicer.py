import threading
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterable

from google.protobuf import empty_pb2 as _empty_pb2
from pynumaflow.shared.server import exit_on_error

from pynumaflow._constants import NUM_THREADS_DEFAULT, STREAM_EOF, _LOGGER, ERR_UDF_EXCEPTION_STRING
from pynumaflow.mapper._dtypes import MapSyncCallable, Datum, MapError
from pynumaflow.proto.mapper import map_pb2, map_pb2_grpc
from pynumaflow.shared.synciter import SyncIterator
from pynumaflow.types import NumaflowServicerContext


class SyncMapServicer(map_pb2_grpc.MapServicer):
    """
    This class is used to create a new grpc Map Servicer instance.
    It implements the SyncMapServicer interface from the proto map.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(self, handler: MapSyncCallable, multiproc: bool = False):
        self.__map_handler: MapSyncCallable = handler
        # This indicates whether the grpc server attached is multiproc or not
        self.multiproc = multiproc
        # create a thread pool for executing UDF code
        self.executor = ThreadPoolExecutor(max_workers=NUM_THREADS_DEFAULT)
        # Thread-safe event to track shutdown state and prevent race conditions
        self._shutdown_event = threading.Event()
        self._shutdown_lock = threading.Lock()  # NEW: lock for shutdown/error handling

    def _handle_error(self, context, error):
        """
        Ensures only one thread triggers shutdown and error reporting.
        """
        with self._shutdown_lock:
            if not self._shutdown_event.is_set():
                self._shutdown_event.set()
                exit_on_error(
                    context, f"{ERR_UDF_EXCEPTION_STRING}: {repr(error)}", parent=self.multiproc
                )
            else:
                _LOGGER.info("Shutdown already initiated by another thread, exiting quietly")

    def MapFn(
        self,
        request_iterator: Iterable[map_pb2.MapRequest],
        context: NumaflowServicerContext,
    ) -> Iterable[map_pb2.MapResponse]:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        try:
            # The first message to be received should be a valid handshake
            req = next(request_iterator)
            # check if it is a valid handshake req
            if not (req.handshake and req.handshake.sot):
                raise MapError("MapFn: expected handshake as the first message")
            yield map_pb2.MapResponse(handshake=map_pb2.Handshake(sot=True))

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
                    self._handle_error(context, res)
                    return
                # return the result
                yield res

            # wait for the threads to clean-up
            reader_thread.join()
            self.executor.shutdown(cancel_futures=True)

        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            self._handle_error(context, err)
            return

    def _process_requests(
        self,
        context: NumaflowServicerContext,
        request_iterator: Iterable[map_pb2.MapRequest],
        result_queue: SyncIterator,
    ):
        try:
            # read through all incoming requests and submit to the
            # threadpool for invocation
            for request in request_iterator:
                # Check if shutdown has been initiated before submitting new tasks
                if self._shutdown_event.is_set():
                    _LOGGER.info("Shutdown initiated, stopping request processing")
                    break
                _ = self.executor.submit(self._invoke_map, context, request, result_queue)
            # wait for all tasks to finish after all requests exhausted
            self.executor.shutdown(wait=True)
            # Indicate to the result queue that no more messages left to process
            result_queue.put(STREAM_EOF)
        except BaseException:
            _LOGGER.critical("MapFn Error, re-raising the error", exc_info=True)

    def _invoke_map(
        self,
        context: NumaflowServicerContext,
        request: map_pb2.MapRequest,
        result_queue: SyncIterator,
    ):
        try:
            # Check if shutdown has been initiated before processing
            if self._shutdown_event.is_set():
                _LOGGER.info("Shutdown initiated, skipping map invocation")
                return

            d = Datum(
                keys=list(request.request.keys),
                value=request.request.value,
                event_time=request.request.event_time.ToDatetime(),
                watermark=request.request.watermark.ToDatetime(),
                headers=dict(request.request.headers),
            )

            responses = self.__map_handler(list(request.request.keys), d)
            results = []
            for resp in responses:
                results.append(
                    map_pb2.MapResponse.Result(
                        keys=list(resp.keys),
                        value=resp.value,
                        tags=resp.tags,
                    )
                )
            result_queue.put(map_pb2.MapResponse(results=results, id=request.id))

        except BaseException as e:
            _LOGGER.critical("MapFn handler error", exc_info=True)
            # Only put the exception in the queue if shutdown hasn't been initiated
            if not self._shutdown_event.is_set():
                result_queue.put(e)
            else:
                _LOGGER.info("Shutdown already initiated, not queuing additional error")
            return

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return map_pb2.ReadyResponse(ready=True)
