from __future__ import annotations

import datetime as _dt
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable

# Re-export the Python ABC for user convenience and typing
from ._reducestreamer_dtypes import ReduceStreamer as ReduceStreamer

class Message:
    keys: list[str] | None
    value: bytes
    tags: list[str] | None

    def __init__(
        self,
        value: bytes,
        keys: list[str] | None = ...,
        tags: list[str] | None = ...,
    ) -> None: ...
    @staticmethod
    def message_to_drop() -> Message: ...

class Datum:
    keys: list[str]
    value: bytes
    watermark: _dt.datetime
    eventtime: _dt.datetime
    headers: dict[str, str]

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class IntervalWindow:
    start: _dt.datetime
    end: _dt.datetime

class Metadata:
    interval_window: IntervalWindow

class PyAsyncDatumStream:
    """
    Python-visible async iterator that yields Datum items from a Tokio mpsc channel.
    """

    def __init__(self) -> None: ...
    def __aiter__(self) -> PyAsyncDatumStream: ...
    def __anext__(self) -> Datum: ...

class ReduceStreamAsyncServer:
    def __init__(
        self,
        sock_file: str = ...,
        info_file: str = ...,
    ) -> None: ...
    def start(
        self,
        py_creator: (
            type[ReduceStreamer]
            | Callable[
                [list[str], AsyncIterable[Datum], Metadata], AsyncIterator[Message]
            ]
        ),
        init_args: tuple | None = ...,
    ) -> Awaitable[None]: ...
    def stop(self) -> None: ...

__all__ = [
    "Datum",
    "IntervalWindow",
    "Message",
    "Metadata",
    "PyAsyncDatumStream",
    "ReduceStreamAsyncServer",
    "ReduceStreamer",
]
