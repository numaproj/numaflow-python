from __future__ import annotations

import datetime as _dt
from typing import Optional, List, Dict, Awaitable

# Re-export the Python ABC for user convenience and typing
from ._session_reduce_dtypes import SessionReducer as SessionReducer


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


class Datum:
    keys: List[str]
    value: bytes
    watermark: _dt.datetime
    eventtime: _dt.datetime
    headers: Dict[str, str]

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class SessionReduceAsyncServer:
    def __init__(
            self,
            sock_file: str | None = ...,
            info_file: str | None = ...,
    ) -> None: ...

    def start(self, py_creator: type, init_args: tuple | None = ...) -> Awaitable[None]: ...

    def stop(self) -> None: ...


__all__ = [
    "Message",
    "Datum",
    "SessionReduceAsyncServer",
    "SessionReducer",
]

