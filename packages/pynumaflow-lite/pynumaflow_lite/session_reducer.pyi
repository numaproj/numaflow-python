from __future__ import annotations

import datetime as _dt
from collections.abc import Awaitable

# Re-export the Python ABC for user convenience and typing
from ._session_reduce_dtypes import SessionReducer as SessionReducer

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

class SessionReduceAsyncServer:
    def __init__(
        self,
        sock_file: str | None = ...,
        info_file: str | None = ...,
    ) -> None: ...
    def start(
        self, py_creator: type[SessionReducer], init_args: tuple | None = ...
    ) -> Awaitable[None]: ...
    def stop(self) -> None: ...

__all__ = [
    "Datum",
    "Message",
    "SessionReduceAsyncServer",
    "SessionReducer",
]
