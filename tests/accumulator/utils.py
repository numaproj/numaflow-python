from datetime import datetime, timezone
from pynumaflow.accumulator import Datum
from pynumaflow.proto.accumulator import accumulator_pb2
from tests.testing_utils import get_time_args


def create_test_datum(keys, value, event_time=None, watermark=None, id_=None, headers=None):
    """Create a test Datum object with default values"""
    if event_time is None:
        event_time = datetime.fromtimestamp(1662998400, timezone.utc)
    if watermark is None:
        watermark = datetime.fromtimestamp(1662998460, timezone.utc)
    if id_ is None:
        id_ = "test_id"
    if headers is None:
        headers = {}
    
    return Datum(
        keys=keys,
        value=value,
        event_time=event_time,
        watermark=watermark,
        id_=id_,
        headers=headers,
    )


def create_accumulator_request(operation, keys, value, event_time=None, watermark=None):
    """Create an AccumulatorRequest for testing"""
    if event_time is None or watermark is None:
        event_time_timestamp, watermark_timestamp = get_time_args()
    else:
        event_time_timestamp = event_time
        watermark_timestamp = watermark
    
    window = accumulator_pb2.KeyedWindow(
        start=event_time_timestamp,
        end=watermark_timestamp,
        slot="slot-0",
        keys=keys,
    )
    
    payload = accumulator_pb2.Payload(
        keys=keys,
        value=value,
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
        id="test_id",
    )
    
    operation_proto = accumulator_pb2.AccumulatorRequest.WindowOperation(
        event=operation,
        keyedWindow=window,
    )
    
    return accumulator_pb2.AccumulatorRequest(
        payload=payload,
        operation=operation_proto,
    )


def create_out_of_order_datums():
    """Create a list of datums that are out of order by event_time"""
    return [
        create_test_datum(
            keys=["test"],
            value=b"event_3",
            event_time=datetime.fromtimestamp(1662998460, timezone.utc),
            id_="3",
        ),
        create_test_datum(
            keys=["test"],
            value=b"event_1",
            event_time=datetime.fromtimestamp(1662998400, timezone.utc),
            id_="1",
        ),
        create_test_datum(
            keys=["test"],
            value=b"event_2",
            event_time=datetime.fromtimestamp(1662998430, timezone.utc),
            id_="2",
        ),
    ]


def create_multi_source_datums():
    """Create datums from multiple sources for stream joining tests"""
    return [
        create_test_datum(
            keys=["source1"],
            value=b"data_from_source1_1",
            event_time=datetime.fromtimestamp(1662998400, timezone.utc),
            id_="s1_1",
        ),
        create_test_datum(
            keys=["source2"],
            value=b"data_from_source2_1",
            event_time=datetime.fromtimestamp(1662998410, timezone.utc),
            id_="s2_1",
        ),
        create_test_datum(
            keys=["source1"],
            value=b"data_from_source1_2",
            event_time=datetime.fromtimestamp(1662998420, timezone.utc),
            id_="s1_2",
        ),
        create_test_datum(
            keys=["source2"],
            value=b"data_from_source2_2",
            event_time=datetime.fromtimestamp(1662998430, timezone.utc),
            id_="s2_2",
        ),
    ]


def create_numeric_datums():
    """Create datums with numeric values for trigger tests"""
    return [
        create_test_datum(
            keys=["test"],
            value=b"1",
            event_time=datetime.fromtimestamp(1662998400, timezone.utc),
            id_="1",
        ),
        create_test_datum(
            keys=["test"],
            value=b"2",
            event_time=datetime.fromtimestamp(1662998410, timezone.utc),
            id_="2",
        ),
        create_test_datum(
            keys=["test"],
            value=b"3",
            event_time=datetime.fromtimestamp(1662998420, timezone.utc),
            id_="3",
        ),
        create_test_datum(
            keys=["test"],
            value=b"4",
            event_time=datetime.fromtimestamp(1662998430, timezone.utc),
            id_="4",
        ),
        create_test_datum(
            keys=["test"],
            value=b"5",
            event_time=datetime.fromtimestamp(1662998440, timezone.utc),
            id_="5",
        ),
    ]


def create_correlation_datums():
    """Create datums for time-based correlation tests"""
    base_time = 1662998400
    return [
        # Group 1: events within 5 seconds
        create_test_datum(
            keys=["correlation"],
            value=b"event_a",
            event_time=datetime.fromtimestamp(base_time, timezone.utc),
            id_="a",
        ),
        create_test_datum(
            keys=["correlation"],
            value=b"event_b",
            event_time=datetime.fromtimestamp(base_time + 2, timezone.utc),
            id_="b",
        ),
        create_test_datum(
            keys=["correlation"],
            value=b"event_c",
            event_time=datetime.fromtimestamp(base_time + 4, timezone.utc),
            id_="c",
        ),
        # Group 2: events within another 5 second window
        create_test_datum(
            keys=["correlation"],
            value=b"event_d",
            event_time=datetime.fromtimestamp(base_time + 15, timezone.utc),
            id_="d",
        ),
        create_test_datum(
            keys=["correlation"],
            value=b"event_e",
            event_time=datetime.fromtimestamp(base_time + 17, timezone.utc),
            id_="e",
        ),
    ]
