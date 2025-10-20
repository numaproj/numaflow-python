from __future__ import annotations

from typing import Optional, List, Dict, Callable, Awaitable, Any
import datetime as _dt

# Re-export the Python ABC for user convenience and typing
from ._map_dtypes import Mapper as Mapper


class Messages:
    def __init__(self) -> None: ...

    def append(self, message: Message) -> None: ...

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


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
    # Read-only attributes provided by the extension
    keys: List[str]
    value: bytes
    watermark: _dt.datetime
    eventtime: _dt.datetime
    headers: Dict[str, str]

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class MapAsyncServer:
    def __init__(
            self,
            sock_file: str | None = ...,
            info_file: str | None = ...,
    ) -> None: ...

    def start(self, py_func: Callable[..., Any]) -> Awaitable[None]: ...

    def stop(self) -> None: ...


# Simple utility function exposed by the extension


__all__ = [
    "Messages",
    "Message",
    "Datum",
    "MapAsyncServer",
    "Mapper",
]
