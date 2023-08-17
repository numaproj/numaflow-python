from dataclasses import dataclass


@dataclass(init=False, repr=False)
class SideInputResponse:
    """
    Class to define the important information for the event.
    Args:
        value: the payload of the event.
    >>> # Example usage
    >>> SideInputResponse(value=b"test_value")
    """

    __slots__ = "_value"

    _value: bytes

    def __init__(
        self,
        value: bytes,
    ):
        self._value = value or b""

    def __str__(self):
        value_string = self._value.decode("utf-8")
        return f"value: {value_string}"

    def __repr__(self):
        return str(self)

    @property
    def value(self) -> bytes:
        """Returns the value of the event."""
        return self._value
