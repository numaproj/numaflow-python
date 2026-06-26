from __future__ import annotations

import datetime as _dt
from collections.abc import AsyncIterable, Awaitable, Callable

# Re-export the Python ABC for user convenience and typing
from ._reduce_dtypes import Reducer as Reducer

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
    @staticmethod
    def to_drop() -> Message: ...

class Messages:
    def __init__(self) -> None: ...
    def append(self, message: Message) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

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

class ReduceAsyncServer:
    def __init__(
        self,
        sock_file: str | None = ...,
        info_file: str | None = ...,
    ) -> None: ...
    def start(
        self,
        py_creator: (
            type[Reducer]
            | Callable[[list[str], AsyncIterable[Datum], Metadata], Awaitable[Messages]]
        ),
        init_args: tuple | None = ...,
    ) -> Awaitable[None]: ...
    def stop(self) -> None: ...

__all__ = [
    "Datum",
    "IntervalWindow",
    "Message",
    "Messages",
    "Metadata",
    "ReduceAsyncServer",
    "Reducer",
]
