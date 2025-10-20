from datetime import datetime
from typing import AsyncIterator, Optional

class Message:
    """
    A message to be sent to the next vertex from an accumulator handler.
    """

    keys: Optional[list[str]]
    value: bytes
    tags: Optional[list[str]]
    id: str
    headers: dict[str, str]
    event_time: datetime
    watermark: datetime

    def __init__(
        self,
        value: bytes,
        keys: list[str] | None = None,
        tags: list[str] | None = None,
        id: str = "",
        headers: dict[str, str] | None = None,
        event_time: datetime | None = None,
        watermark: datetime | None = None,
    ) -> None: ...
    @staticmethod
    def message_to_drop() -> Message:
        """
        Drop a Message, do not forward to the next vertex.
        """
        ...
    @staticmethod
    def from_datum(
        datum: Datum,
        value: bytes | None = None,
        keys: list[str] | None = None,
        tags: list[str] | None = None,
    ) -> Message:
        """
        Create a Message from a Datum, preserving all metadata (id, headers, event_time, watermark).

        Args:
            datum: The source Datum to copy metadata from
            value: Optional new value (defaults to datum.value)
            keys: Optional new keys (defaults to datum.keys)
            tags: Optional tags for conditional forwarding

        Returns:
            A new Message with metadata from the datum
        """
        ...

class Datum:
    """
    The incoming AccumulatorRequest accessible in Python function (streamed).
    """

    keys: list[str]
    value: bytes
    watermark: datetime
    event_time: datetime
    headers: dict[str, str]
    id: str

class AccumulatorAsyncServer:
    """
    Async Accumulator Server that can be started from Python code.
    """

    def __init__(
        self,
        sock_file: str | None = "/var/run/numaflow/accumulator.sock",
        info_file: str | None = "/var/run/numaflow/accumulator-server-info",
    ) -> None: ...
    async def start(
        self, py_creator: object, init_args: object | None = None
    ) -> None:
        """
        Start the server with the given Python class (creator).

        Args:
            py_creator: The Python class to instantiate per key
            init_args: Optional tuple of positional arguments for class instantiation
        """
        ...
    def stop(self) -> None:
        """
        Trigger server shutdown from Python (idempotent).
        """
        ...

class PyAsyncDatumStream:
    """
    Python-visible async iterator that yields Datum items from a Tokio mpsc channel.
    """

    def __init__(self) -> None: ...
    def __aiter__(self) -> PyAsyncDatumStream: ...
    def __anext__(self) -> Datum: ...

class Accumulator:
    """
    Base class for implementing an Accumulator.
    """

    async def handler(self, datums: AsyncIterator[Datum]) -> AsyncIterator[Message]:
        """
        Accumulate can read unordered from the input stream and emit the ordered data to the output stream.

        Args:
            datums: An async iterator of Datum objects

        Yields:
            Message objects to be sent to the next vertex
        """
        ...

