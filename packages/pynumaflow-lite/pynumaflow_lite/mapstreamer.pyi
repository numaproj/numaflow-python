from __future__ import annotations

import datetime as _dt
from collections.abc import AsyncIterator, Awaitable, Callable

class NackOptions:
    """Per-message redelivery options for a nack."""

    delay: int | None
    max_deliveries: int | None
    reason: str | None
    nack_map: dict[str, str]

    def __init__(
        self,
        delay: int | None = ...,
        max_deliveries: int | None = ...,
        reason: str | None = ...,
        nack_map: dict[str, str] | None = ...,
    ) -> None: ...
    def __repr__(self) -> str: ...

class Message:
    keys: list[str] | None
    value: bytes
    tags: list[str] | None
    nack_options: NackOptions | None

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
    @staticmethod
    def to_nack(nack_options: NackOptions | None = ...) -> Message: ...
    @staticmethod
    def to_fail() -> Message: ...

class Datum:
    keys: list[str]
    value: bytes
    watermark: _dt.datetime
    eventtime: _dt.datetime
    headers: dict[str, str]

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
    "Datum",
    "MapStreamAsyncServer",
    "MapStreamer",
    "Message",
    "NackOptions",
]
