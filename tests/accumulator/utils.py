from datetime import datetime, timezone
from pynumaflow.accumulator import Datum


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
