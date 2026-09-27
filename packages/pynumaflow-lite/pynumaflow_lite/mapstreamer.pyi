from __future__ import annotations

import datetime as _dt
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType

from ._mapstream_dtypes import MapStreamer as MapStreamer

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
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
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
    event_time: _dt.datetime
    headers: dict[str, str]

    def __init__(
        self,
        *,
        keys: list[str] | None = ...,
        value: bytes | None = ...,
        event_time: _dt.datetime | None = ...,
        watermark: _dt.datetime | None = ...,
        headers: dict[str, str] | None = ...,
    ) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class _MapStreamAsyncServer:
    def __init__(
        self,
        sock_file: str | None = ...,
        server_info_file: str | None = ...,
    ) -> None: ...
    def start(self, handler: Callable[[Datum], AsyncIterator[Message]]) -> Awaitable[None]: ...
    def wait_ready(self, timeout: float = ...) -> Awaitable[None]: ...
    def stop(self) -> None: ...

class MapStreamAsyncServer:
    def __init__(
        self,
        handler: Callable[[Datum], AsyncIterator[Message]],
        *,
        sock_file: str | None = ...,
        server_info_file: str | None = ...,
        install_signal_handlers: bool = ...,
    ) -> None: ...
    def run(self) -> None: ...
    async def serve(self) -> None: ...
    def stop(self) -> None: ...
    async def wait_ready(self, timeout: float = ...) -> None: ...
    async def wait_for_termination(self) -> None: ...
    async def __aenter__(self) -> MapStreamAsyncServer: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

__all__ = [
    "Datum",
    "MapStreamAsyncServer",
    "MapStreamer",
    "Message",
    "NackOptions",
]
