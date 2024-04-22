import json
from datetime import datetime, timezone
from pynumaflow.info.types import EOF
from google.protobuf import timestamp_pb2 as _timestamp_pb2


def get_time_args() -> (datetime, datetime):
    event_time_timestamp = _timestamp_pb2.Timestamp()
    event_time_timestamp.FromDatetime(dt=mock_event_time())
    watermark_timestamp = _timestamp_pb2.Timestamp()
    watermark_timestamp.FromDatetime(dt=mock_watermark())
    return event_time_timestamp, watermark_timestamp


def mock_message():
    msg = bytes("test_mock_message", encoding="utf-8")
    return msg


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


def mock_new_event_time():
    t = datetime.fromtimestamp(1663098400, timezone.utc)
    return t


def mock_watermark():
    t = datetime.fromtimestamp(1662998460, timezone.utc)
    return t


def mock_headers():
    headers = {"key1": "value1", "key2": "value2"}
    return headers


def mock_interval_window_start():
    event_time_timestamp = _timestamp_pb2.Timestamp()
    t = datetime.fromtimestamp(1662998400000 / 1e3, timezone.utc)
    event_time_timestamp.FromDatetime(dt=t)
    # t = datetime.fromtimestamp(1662998400000, timezone.utc)
    return event_time_timestamp


def mock_interval_window_end():
    event_time_timestamp = _timestamp_pb2.Timestamp()
    t = datetime.fromtimestamp(1662998460000 / 1e3, timezone.utc)
    event_time_timestamp.FromDatetime(dt=t)
    # t = datetime.fromtimestamp(1662998460000, timezone.utc)
    return event_time_timestamp


def mock_start_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


def mock_end_time():
    t = datetime.fromtimestamp(1662998520, timezone.utc)
    return t


def read_info_server(info_file: str):
    f = open(info_file)
    retry = 10
    res = f.read()
    a, b = info_serv_is_ready(info_serv_data=res)
    while (a is not True) and retry > 0:
        a, b = info_serv_is_ready(info_serv_data=res)

    a, b = info_serv_is_ready(info_serv_data=res)
    if a:
        res = json.loads(b)
        return res

    else:
        return None


def info_serv_is_ready(info_serv_data: str, eof: str = EOF):
    if len(info_serv_data) < len(eof):
        return False
    len_diff = len(info_serv_data) - len(eof)
    last_char = info_serv_data[len_diff:]
    if last_char == EOF:
        data = info_serv_data[:len_diff]
        return True, data
    return False, None


def mock_terminate_on_stop(process):
    print("Mock terminate ", process)
