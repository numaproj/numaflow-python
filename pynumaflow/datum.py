from datetime import datetime


class Datum:
    """Defines the important information for the event."""

    def __init__(self, value: bytes, event_time: datetime, watermark: datetime):
        self._value = value or b""
        if not isinstance(event_time, datetime):
            raise TypeError(f"Wrong data type: {type(event_time)} for Datum.event_time")
        self._event_time = event_time
        if not isinstance(watermark, datetime):
            raise TypeError(f"Wrong data type: {type(watermark)} for Datum.watermark")
        self._watermark = watermark

    def __str__(self):
        value_string = self._value.decode("utf-8")
        return f"value: {value_string}, event_time: {str(self._event_time)}, watermark: {str(self._watermark)}"

    def __repr__(self):
        return str(self)

    @property
    def value(self):
        """Returns the value of the event."""
        return self._value

    @property
    def event_time(self):
        """Returns the event time of the event."""
        return self._event_time

    @property
    def watermark(self):
        """Returns the watermark of the event."""
        return self._watermark
