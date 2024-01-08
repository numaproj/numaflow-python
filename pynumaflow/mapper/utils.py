import grpc
from pynumaflow.mapper._dtypes import MapCallable

from pynumaflow.mapper import Datum
from pynumaflow.mapper.proto import map_pb2
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER


def _map_fn_util(
    __map_handler: MapCallable, request: map_pb2.MapRequest, context: NumaflowServicerContext
) -> map_pb2.MapResponse:
    # proto repeated field(keys) is of type google._upb._message.RepeatedScalarContainer
    # we need to explicitly convert it to list
    try:
        msgs = __map_handler(
            list(request.keys),
            Datum(
                keys=list(request.keys),
                value=request.value,
                event_time=request.event_time.ToDatetime(),
                watermark=request.watermark.ToDatetime(),
            ),
        )
    except Exception as err:
        _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
        context.set_code(grpc.StatusCode.UNKNOWN)
        context.set_details(str(err))
        return map_pb2.MapResponse(results=[])

    datums = []

    for msg in msgs:
        datums.append(map_pb2.MapResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags))

    return map_pb2.MapResponse(results=datums)