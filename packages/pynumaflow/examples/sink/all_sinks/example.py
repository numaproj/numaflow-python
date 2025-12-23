import os
from collections.abc import AsyncIterable
from pynumaflow.sinker import Datum, Responses, Response, Sinker, Message
from pynumaflow.sinker import SinkAsyncServer
import logging
import random

logging.basicConfig(level=logging.DEBUG)
_LOGGER = logging.getLogger(__name__)


class UserDefinedSink(Sinker):
    async def handler(self, datums: AsyncIterable[Datum]) -> Responses:
        responses = Responses()
        async for msg in datums:
            if primary_sink_write_status():
                _LOGGER.info(
                    "Write to User Defined Sink succeeded, writing %s to onSuccess sink",
                    msg.value.decode("utf-8"),
                )
                # create a message to be sent to onSuccess sink
                on_success_message = Response.as_on_success(
                    msg.id,
                    Message(msg.value, ["on_success"], msg.user_metadata),
                )
                responses.append(on_success_message)
                # Sending `None`, on the other hand, specifies that simply send
                # the original message to the onSuccess sink
                # `responses.append(Response.as_on_success(msg.id, None))`
            else:
                _LOGGER.info(
                    "Write to User Defined Sink failed, writing %s to fallback sink",
                    msg.value.decode("utf-8"),
                )
                responses.append(Response.as_fallback(msg.id))
        return responses


async def udsink_handler(datums: AsyncIterable[Datum]) -> Responses:
    responses = Responses()
    async for msg in datums:
        if primary_sink_write_status():
            _LOGGER.info(
                "Write to User Defined Sink succeeded, writing %s to onSuccess sink",
                msg.value.decode("utf-8"),
            )
            # create a message to be sent to onSuccess sink
            on_success_message = Response.as_on_success(
                msg.id,
                Message(msg.value).with_keys(["on_success"]).with_user_metadata(msg.user_metadata),
            )
            responses.append(on_success_message)
            # Sending `None`, on the other hand, specifies that simply send
            # the original message to the onSuccess sink
            # `responses.append(Response.as_on_success(msg.id, None))`
        else:
            _LOGGER.info(
                "Write to User Defined Sink failed, writing %s to fallback sink",
                msg.value.decode("utf-8"),
            )
            responses.append(Response.as_fallback(msg.id))
    return responses


def primary_sink_write_status():
    # simulate writing to primary sink and return status of the same
    # return True if writing to primary sink succeeded
    # return False if writing to primary sink failed
    return random.randint(0, 1) == 1


if __name__ == "__main__":
    invoke = os.getenv("INVOKE", "func_handler")
    if invoke == "class":
        sink_handler = UserDefinedSink()
    else:
        sink_handler = udsink_handler
    grpc_server = SinkAsyncServer(sink_handler)
    grpc_server.start()
