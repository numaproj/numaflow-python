from pynumaflow.batchmapper import Datum
from pynumaflow.proto.batchmapper import batchmap_pb2
from tests.testing_utils import get_time_args, mock_message


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        # add the id to the datum
        request.id = str(i)
        if resetkey:
            request.payload.keys.extend([f"key-{i}"])
        yield request


def start_request() -> Datum:
    event_time_timestamp, watermark_timestamp = get_time_args()
    request = batchmap_pb2.BatchMapRequest(
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
    )
    return request
