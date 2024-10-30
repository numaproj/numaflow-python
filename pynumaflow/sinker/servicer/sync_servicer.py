from collections.abc import Iterable


from pynumaflow._constants import _LOGGER, STREAM_EOF
from pynumaflow.proto.sinker import sink_pb2_grpc, sink_pb2
from pynumaflow.shared.server import exit_on_error
from pynumaflow.shared.synciter import SyncIterator
from pynumaflow.shared.thread_with_return import ThreadWithReturnValue
from pynumaflow.sinker._dtypes import SinkSyncCallable
from pynumaflow.sinker.servicer.utils import (
    datum_from_sink_req,
    _create_read_handshake_response,
    build_sink_resp_results,
)
from pynumaflow.types import NumaflowServicerContext


class SyncSinkServicer(sink_pb2_grpc.SinkServicer):
    """
    This class is used to create a new grpc Sink servicer instance.
    It implements the SinkServicer interface from the proto sink.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(self, handler: SinkSyncCallable):
        self.handler: SinkSyncCallable = handler

    def SinkFn(
        self, request_iterator: Iterable[sink_pb2.SinkRequest], context: NumaflowServicerContext
    ) -> Iterable[sink_pb2.SinkResponse]:
        """
        Applies a sink function to datum elements.
        """

        try:
            # The first message to be received should be a valid handshake
            req = next(request_iterator)
            # check if it is a valid handshake req
            if not (req.handshake and req.handshake.sot):
                raise Exception("SinkFn: expected handshake message")
            yield _create_read_handshake_response()
            # cur_task is used to track the thread processing
            # the current batch of messages.
            cur_task = None
            # Use a queue backed to handle request batches
            req_queue = SyncIterator()

            # iterate of the incoming messages ot the sink
            for d in request_iterator:
                # if we do not have any active thread currently processing the batch
                # we need to create one and call the User function for processing the same.
                if cur_task is None:
                    # Use a queue to handle request batches
                    req_queue = SyncIterator()
                    cur_task = ThreadWithReturnValue(
                        target=self._invoke_sink, args=(req_queue, context)
                    )
                    cur_task.start()

                # when we have end of transmission message, we need to stop the processing the
                # current batch and wait for the next batch of messages.
                # We will also wait for the current task to finish processing the current batch.
                # We mark the current task as None to indicate that we are
                # ready to process the next batch.
                if d.status and d.status.eot:
                    req_queue.put(STREAM_EOF)
                    ret = cur_task.join()
                    yield sink_pb2.SinkResponse(results=ret)
                    # send EOT after each finishing sink responses
                    yield sink_pb2.SinkResponse(status=sink_pb2.TransmissionStatus(eot=True))
                    cur_task = None
                    continue

                # if we have a valid message, we will add it to the request queue for processing.
                datum = datum_from_sink_req(d)
                req_queue.put(datum)

            if cur_task:
                cur_task.join()

        except BaseException as err:
            # Handle exceptions
            err_msg = f"UDSinkError: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            exit_on_error(context, err_msg)
            return

    def _invoke_sink(self, request_queue: SyncIterator, context: NumaflowServicerContext):
        try:
            # Invoke the handler function with the request queue
            rspns = self.handler(request_queue.read_iterator())
            return build_sink_resp_results(rspns)
        except BaseException as err:
            err_msg = f"UDSinkError: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            exit_on_error(context, err_msg)
            raise err

    def IsReady(self, request, context: NumaflowServicerContext) -> sink_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        """
        return sink_pb2.ReadyResponse(ready=True)
