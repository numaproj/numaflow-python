from pynumaflow.proto.sourcetransformer import transform_pb2
from pynumaflow.sourcetransformer import Datum, Messages, Message
from tests.testing_utils import mock_new_event_time, mock_message, get_time_args


def transform_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    msg = "payload:{} event_time:{} ".format(
        val.decode("utf-8"),
        datum.event_time,
    )
    val = bytes(msg, encoding="utf-8")
    messages = Messages()
    messages.append(Message(val, mock_new_event_time(), keys=keys))
    return messages


def err_transform_handler(_: list[str], __: Datum) -> Messages:
    raise RuntimeError("Something is fishy!")


def get_test_datums(handshake=True):
    event_time_timestamp, watermark_timestamp = get_time_args()

    responses = []

    if handshake:
        responses.append(
            transform_pb2.SourceTransformRequest(
                handshake=transform_pb2.Handshake(sot=True),
            )
        )

    test_datum = [
        transform_pb2.SourceTransformRequest(
            request=transform_pb2.SourceTransformRequest.Request(
                keys=["test"],
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                id="test-id-1",
            )
        ),
        transform_pb2.SourceTransformRequest(
            request=transform_pb2.SourceTransformRequest.Request(
                keys=["test"],
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                id="test-id-2",
            )
        ),
        transform_pb2.SourceTransformRequest(
            request=transform_pb2.SourceTransformRequest.Request(
                keys=["test"],
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                id="test-id-3",
            )
        ),
    ]
    for x in test_datum:
        responses.append(x)
    return responses
