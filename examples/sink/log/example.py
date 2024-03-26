import os
from collections.abc import Iterator
from pynumaflow.sinker import Datum, Responses, Response, SinkServer
from pynumaflow.sinker import Sinker
import logging

logging.basicConfig(level=logging.DEBUG)
_LOGGER = logging.getLogger(__name__)


class UserDefinedSink(Sinker):
    def handler(self, datums: Iterator[Datum]) -> Responses:
        responses = Responses()
        for msg in datums:
            _LOGGER.info("User Defined Sink %s", msg.value.decode("utf-8"))
            responses.append(Response.as_success(msg.id))
        return responses


def udsink_handler(datums: Iterator[Datum]) -> Responses:
    responses = Responses()
    for msg in datums:
        _LOGGER.info(
            "User Defined Sink: Payload %s , Headers %s", msg.value.decode("utf-8"), msg.headers
        )
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    invoke = os.getenv("INVOKE", "func_handler")
    if invoke == "class":
        sink_handler = UserDefinedSink()
    else:
        sink_handler = udsink_handler
    grpc_server = SinkServer(sink_handler)
    grpc_server.start()
