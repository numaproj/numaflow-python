from pynumaflow.mapstreamer import Datum
from pynumaflow.mapstreamer.proto import mapstream_pb2
from tests.testing_utils import get_time_args, mock_message


def start_request_map_stream() -> (Datum, tuple):
    event_time_timestamp, watermark_timestamp = get_time_args()
    request = mapstream_pb2.MapStreamRequest(
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
    )

    return request
