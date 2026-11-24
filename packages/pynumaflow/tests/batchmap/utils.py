from pynumaflow.proto.mapper import map_pb2
from tests.testing_utils import get_time_args, mock_message, mock_headers, mock_request_metadata


def request_generator(count, session=1, handshake=True):
    event_time_timestamp, watermark_timestamp = get_time_args()

    if handshake:
        yield map_pb2.MapRequest(handshake=map_pb2.Handshake(sot=True))

    for j in range(session):
        for i in range(count):
            req = map_pb2.MapRequest(
                request=map_pb2.MapRequest.Request(
                    value=mock_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                    headers=mock_headers(),
                ),
                id="test-id-" + str(i),
            )
            yield req

        yield map_pb2.MapRequest(status=map_pb2.TransmissionStatus(eot=True))


def metadata_request_generator():
    event_time_timestamp, watermark_timestamp = get_time_args()

    yield map_pb2.MapRequest(handshake=map_pb2.Handshake(sot=True))
    yield map_pb2.MapRequest(
        request=map_pb2.MapRequest.Request(
            value=b"with-metadata",
            event_time=event_time_timestamp,
            watermark=watermark_timestamp,
            metadata=mock_request_metadata(),
        ),
        id="with-metadata",
    )
    yield map_pb2.MapRequest(
        request=map_pb2.MapRequest.Request(
            value=b"no-metadata",
            event_time=event_time_timestamp,
            watermark=watermark_timestamp,
        ),
        id="no-metadata",
    )
    yield map_pb2.MapRequest(status=map_pb2.TransmissionStatus(eot=True))
