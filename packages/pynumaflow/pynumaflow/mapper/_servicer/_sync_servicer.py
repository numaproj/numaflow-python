import threading
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from pynumaflow.shared.server import update_context_err
from pynumaflow._metadata import _user_and_system_metadata_from_proto

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
        # Graceful shutdown: when set, a watcher thread in _run_server() calls
        # server.stop() instead of hard-killing the process via psutil.
        self.shutdown_event: threading.Event = threading.Event()
        self.error: BaseException | None = None

    def MapFn(
        self,
        request_iterator: Iterator[map_pb2.MapRequest],
        context: NumaflowServicerContext,
    ) -> Iterator[map_pb2.MapResponse]:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        result_queue = None
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
                    if isinstance(res, grpc.RpcError):
                        # Client disconnected mid-stream — the reader thread
                        # surfaced the error via the queue. Not a UDF fault.
                        _LOGGER.warning("gRPC stream closed, shutting down the server.")
                        result_queue.close()
                        self.shutdown_event.set()
                        return
                    err_msg = f"{ERR_UDF_EXCEPTION_STRING}: {repr(res)}"
                    update_context_err(context, res, err_msg)
                    # Unblock the reader thread if it is waiting on queue.put()
                    result_queue.close()
                    self.error = res
                    self.shutdown_event.set()
                    return
                # return the result
                yield res

            # wait for the threads to clean-up
            reader_thread.join()
            self.executor.shutdown(cancel_futures=True)

        except grpc.RpcError:
            # Client disconnected — not a UDF error, but we still need to
            # shut down the server so the process can exit cleanly.
            _LOGGER.warning("gRPC stream closed, shutting down the server.")
            if result_queue is not None:
                result_queue.close()
            self.shutdown_event.set()
            return

        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            err_msg = f"{ERR_UDF_EXCEPTION_STRING}: {repr(err)}"
            update_context_err(context, err, err_msg)
            if result_queue is not None:
                result_queue.close()
            self.error = err
            self.shutdown_event.set()
            return

    def _process_requests(
        self,
        context: NumaflowServicerContext,
        request_iterator: Iterator[map_pb2.MapRequest],
        result_queue: SyncIterator,
    ):
        try:
            # read through all incoming requests and submit to the
            # threadpool for invocation
            for request in request_iterator:
                _ = self.executor.submit(self._invoke_map, context, request, result_queue)
            # wait for all tasks to finish after all requests exhausted
            self.executor.shutdown(wait=True)
            # Indicate to the result queue that no more messages left to process
            result_queue.put(STREAM_EOF)
        except BaseException as e:
            _LOGGER.critical("MapFn Error, re-raising the error", exc_info=True)
            # Surface the error to the consumer; MapFn will handle and exit
            result_queue.put(e)

    def _invoke_map(
        self,
        context: NumaflowServicerContext,
        request: map_pb2.MapRequest,
        result_queue: SyncIterator,
    ):
        try:
            user_metadata, system_metadata = _user_and_system_metadata_from_proto(
                request.request.metadata
            )
            d = Datum(
                keys=list(request.request.keys),
                value=request.request.value,
                event_time=request.request.event_time.ToDatetime(),
                watermark=request.request.watermark.ToDatetime(),
                headers=dict(request.request.headers),
                user_metadata=user_metadata,
                system_metadata=system_metadata,
            )

            responses = self.__map_handler(list(request.request.keys), d)
            results = []
            for resp in responses:
                results.append(
                    map_pb2.MapResponse.Result(
                        keys=list(resp.keys),
                        value=resp.value,
                        tags=resp.tags,
                        metadata=resp.user_metadata._to_proto(),
                    )
                )
            result_queue.put(map_pb2.MapResponse(results=results, id=request.id))

        except BaseException as e:
            _LOGGER.critical("MapFn handler error", exc_info=True)
            result_queue.put(e)
            return

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return map_pb2.ReadyResponse(ready=True)
