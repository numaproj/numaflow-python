from __future__ import annotations

import datetime as _dt
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType

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
    def to_drop() -> Message: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...

class Datum:
    keys: list[str]
    value: bytes
    watermark: _dt.datetime
    event_time: _dt.datetime
    id: str
    headers: dict[str, str]

    def __init__(
        self,
        *,
        keys: list[str] = ...,
        value: bytes = ...,
        id: str = ...,
        event_time: _dt.datetime | None = ...,
        watermark: _dt.datetime | None = ...,
        headers: dict[str, str] = ...,
    ) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class BatchResponse:
    id: str
    messages: list[Message]

    def __init__(self, id: str, messages: list[Message] | None = ...) -> None: ...
    def append(self, message: Message) -> None: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...

class _BatchMapAsyncServer:
    def __init__(
        self,
        sock_file: str | None = ...,
        server_info_file: str | None = ...,
    ) -> None: ...
    def start(
        self,
        handler: Callable[[AsyncIterator[Datum]], Awaitable[list[BatchResponse]]],
    ) -> Awaitable[None]: ...
    def wait_ready(self, timeout: float = ...) -> Awaitable[None]: ...
    def stop(self) -> None: ...

class BatchMapAsyncServer:
    def __init__(
        self,
        handler: Callable[[AsyncIterator[Datum]], Awaitable[list[BatchResponse]]],
        *,
        sock_file: str | None = ...,
        server_info_file: str | None = ...,
    ) -> None: ...
    def run(self) -> None: ...
    async def serve(self) -> None: ...
    def stop(self) -> None: ...
    async def wait_ready(self, timeout: float = ...) -> None: ...
    async def __aenter__(self) -> BatchMapAsyncServer: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

__all__ = [
    "BatchMapAsyncServer",
    "BatchResponse",
    "Datum",
    "Message",
]
