from __future__ import annotations

from typing import Optional, List, Dict, Callable, Awaitable, Any
import datetime as _dt

# Re-export the Python ABC for user convenience and typing
from ._source_dtypes import Sourcer as Sourcer


class Message:
    """A message to be sent from the source."""
    payload: bytes
    offset: Offset
    event_time: _dt.datetime
    keys: List[str]
    headers: Dict[str, str]

    def __init__(
        self,
        payload: bytes,
        offset: Offset,
        event_time: _dt.datetime,
        keys: Optional[List[str]] = ...,
        headers: Optional[Dict[str, str]] = ...,
    ) -> None: ...

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class Offset:
    """The offset of a message."""
    offset: bytes
    partition_id: int

    def __init__(
        self,
        offset: bytes,
        partition_id: int = ...,
    ) -> None: ...

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class ReadRequest:
    """A request to read messages from the source."""
    num_records: int
    timeout_ms: int

    def __init__(
        self,
        num_records: int,
        timeout_ms: int = ...,
    ) -> None: ...

    def __repr__(self) -> str: ...


class AckRequest:
    """A request to acknowledge messages."""
    offsets: List[Offset]

    def __init__(
        self,
        offsets: List[Offset],
    ) -> None: ...

    def __repr__(self) -> str: ...


class NackRequest:
    """A request to negatively acknowledge messages."""
    offsets: List[Offset]

    def __init__(
        self,
        offsets: List[Offset],
    ) -> None: ...

    def __repr__(self) -> str: ...


class PendingResponse:
    """Response for pending messages count."""
    count: int

    def __init__(
        self,
        count: int = ...,
    ) -> None: ...

    def __repr__(self) -> str: ...


class PartitionsResponse:
    """Response for partitions."""
    partitions: List[int]

    def __init__(
        self,
        partitions: List[int],
    ) -> None: ...

    def __repr__(self) -> str: ...


class SourceAsyncServer:
    """Async Source Server that can be started from Python code."""

    def __init__(
        self,
        sock_file: str | None = ...,
        info_file: str | None = ...,
    ) -> None: ...

    def start(self, py_func: Callable[..., Any]) -> Awaitable[None]: ...

    def stop(self) -> None: ...


__all__ = [
    "Message",
    "Offset",
    "ReadRequest",
    "AckRequest",
    "NackRequest",
    "PendingResponse",
    "PartitionsResponse",
    "SourceAsyncServer",
    "Sourcer",
]

