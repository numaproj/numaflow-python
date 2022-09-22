import unittest
from datetime import datetime, timezone
from google.protobuf import timestamp_pb2 as _timestamp_pb2


from pynumaflow.sink._dtypes import (
    Datum,
)


def mock_message():
    msg = bytes("test_mock_message", encoding="utf-8")
    return msg


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


def mock_watermark():
    t = datetime.fromtimestamp(1662998460, timezone.utc)
    return t


class TestDatum(unittest.TestCase):
    def test_err_event_time(self):
        ts = _timestamp_pb2.Timestamp()
        ts.GetCurrentTime()
        with self.assertRaises(Exception) as context:
            Datum(sink_msg_id="test_id_0", value=mock_message(), event_time=ts, watermark=ts)
        self.assertEqual(
            "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
            "for Datum.event_time",
            str(context.exception),
        )

    def test_err_watermark(self):
        ts = _timestamp_pb2.Timestamp()
        ts.GetCurrentTime()
        with self.assertRaises(Exception) as context:
            Datum(
                sink_msg_id="test_id_0",
                value=mock_message(),
                event_time=mock_event_time(),
                watermark=ts,
            )
        self.assertEqual(
            "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
            "for Datum.watermark",
            str(context.exception),
        )

    def test_value(self):
        d = Datum(
            sink_msg_id="test_id_0",
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
        )
        self.assertEqual(mock_message(), d.value)
        self.assertEqual(
            "id: test_id_0, value: test_mock_message, "
            "event_time: 2022-09-12 16:00:00+00:00, watermark: 2022-09-12 16:01:00+00:00",
            str(d),
        )
        self.assertEqual(
            "id: test_id_0, value: test_mock_message, "
            "event_time: 2022-09-12 16:00:00+00:00, "
            "watermark: 2022-09-12 16:01:00+00:00",
            repr(d),
        )

    def test_id(self):
        d = Datum(
            sink_msg_id="test_id_0",
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
        )
        self.assertEqual("test_id_0", d.id)

    def test_event_time(self):
        d = Datum(
            sink_msg_id="test_id_0",
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
        )
        self.assertEqual(mock_event_time(), d.event_time)

    def test_watermark(self):
        d = Datum(
            sink_msg_id="test_id_0",
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
        )
        self.assertEqual(mock_watermark(), d.watermark)


if __name__ == "__main__":
    unittest.main()
