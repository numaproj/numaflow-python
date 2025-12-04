from __future__ import annotations

from typing import Optional, List, Dict, Callable, Awaitable, Any
import datetime as _dt

# Re-export the Python ABC for user convenience and typing
from ._sourcetransformer_dtypes import SourceTransformer as SourceTransformer


class Messages:
    def __init__(self) -> None: ...

    def append(self, message: Message) -> None: ...

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class Message:
    keys: Optional[List[str]]
    value: bytes
    event_time: _dt.datetime
    tags: Optional[List[str]]

    def __init__(
            self,
            value: bytes,
            event_time: _dt.datetime,
            keys: Optional[List[str]] = ...,
            tags: Optional[List[str]] = ...,
    ) -> None: ...

    @staticmethod
    def message_to_drop(event_time: _dt.datetime) -> Message: ...


class Datum:
    # Read-only attributes provided by the extension
    keys: List[str]
    value: bytes
    watermark: _dt.datetime
    event_time: _dt.datetime
    headers: Dict[str, str]

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class SourceTransformAsyncServer:
    def __init__(
            self,
            sock_file: str | None = ...,
            info_file: str | None = ...,
    ) -> None: ...

    def start(self, py_func: Callable[..., Any]) -> Awaitable[None]: ...

    def stop(self) -> None: ...


__all__ = [
    "Messages",
    "Message",
    "Datum",
    "SourceTransformAsyncServer",
    "SourceTransformer",
]

