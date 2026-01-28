from __future__ import annotations

from typing import Optional, List, Dict, Callable, Awaitable, AsyncIterator
import datetime as _dt

# Re-export the Python ABC for user convenience and typing
from ._stream_dtypes import MapStreamer as MapStreamer


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


class Datum:
    keys: List[str]
    value: bytes
    watermark: _dt.datetime
    eventtime: _dt.datetime
    headers: Dict[str, str]

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class MapStreamAsyncServer:
    def __init__(
            self,
            sock_file: str | None = ...,
            info_file: str | None = ...,
    ) -> None: ...

    def start(self, py_func: Callable[[list[str], Datum], AsyncIterator[Message]]) -> Awaitable[None]: ...

    def stop(self) -> None: ...


class MapStreamer:
    async def handler(self, keys: list[str], datum: Datum) -> AsyncIterator[Message]: ...


__all__ = [
    "Message",
    "Datum",
    "MapStreamAsyncServer",
    "MapStreamer",
]
