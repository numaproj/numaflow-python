import threading
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from pynumaflow.shared.server import update_context_err
from pynumaflow._metadata import _user_and_system_metadata_from_proto

from pynumaflow._constants import NUM_THREADS_DEFAULT, _LOGGER, ERR_UDF_EXCEPTION_STRING
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
            _LOGGER.warning("gRPC stream closed, shutting down the server.")
            if result_queue is not None:
                result_queue.close()
            self.shutdown_event.set()
            return

        except BaseException as err:
            err_msg = f"UDFError, {ERR_UDF_EXCEPTION_STRING}: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            update_context_err(context, err, err_msg)
            # Unblock the reader thread if it is waiting on queue.put()
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
            result_queue.close()
        except grpc.RpcError:
            # The only error that can occur here is the gRPC stream closing
            # (e.g. client disconnected). UDF exceptions are caught inside _invoke_map
            # and never propagate here.
            _LOGGER.warning("gRPC stream closed in reader thread, shutting down the server.")
            # Let already-submitted UDF tasks finish within the graceful shutdown period
            self.executor.shutdown(wait=True)
            # Signal MapFn's read_iterator() loop to exit cleanly
            result_queue.close()
            # Trigger server shutdown (not a UDF error, so self.error is not set)
            self.shutdown_event.set()
        except BaseException as e:
            _LOGGER.critical("MapFn Error while reading requests from gRPC stream", exc_info=True)
            # Surface the error to the consumer; MapFn will handle and exit
            result_queue.put(e)

    def _invoke_map(
        self,
        context: NumaflowServicerContext,
        request: map_pb2.MapRequest,
        result_queue: SyncIterator,
    ):
        try:
            # Skip processing if a shutdown is already in progress
            # (e.g. a prior invocation raised an exception)
            if self.shutdown_event.is_set():
                return

            (user_metadata, system_metadata) = _user_and_system_metadata_from_proto(
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
