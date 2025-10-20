from __future__ import annotations

import datetime as _dt
from typing import Optional, List, Dict, Awaitable

# Re-export the Python ABC for user convenience and typing
from ._reduce_dtypes import Reducer as Reducer


class Message:
    keys: Optional[List[str]]
    value: bytes
    tags: Optional[List[str]]

    def __init__(
            self,
            value: bytes,
            keys: Optional[List[str]] = ...,
            tags: Optional[List[str]] = ...,
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
    keys: List[str]
    value: bytes
    watermark: _dt.datetime
    eventtime: _dt.datetime
    headers: Dict[str, str]

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

    def start(self, py_creator: type, init_args: tuple | None = ...) -> Awaitable[None]: ...

    def stop(self) -> None: ...


__all__ = [
    "Message",
    "Messages",
    "Datum",
    "IntervalWindow",
    "Metadata",
    "ReduceAsyncServer",
    "Reducer",
]
