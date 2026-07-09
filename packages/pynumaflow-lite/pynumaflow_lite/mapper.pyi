from __future__ import annotations

import datetime as _dt
from collections.abc import Awaitable, Callable
from types import TracebackType

class Message:
    keys: list[str] | None
    value: bytes
    tags: list[str] | None
    user_metadata: dict[str, dict[str, bytes]] | None

    def __init__(
        self,
        value: bytes,
        keys: list[str] | None = ...,
        tags: list[str] | None = ...,
        user_metadata: dict[str, dict[str, bytes]] | None = ...,
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
    headers: dict[str, str]
    user_metadata: dict[str, dict[str, bytes]]
    system_metadata: dict[str, dict[str, bytes]]

    def __init__(
        self,
        *,
        keys: list[str] = ...,
        value: bytes = ...,
        event_time: _dt.datetime | None = ...,
        watermark: _dt.datetime | None = ...,
        headers: dict[str, str] = ...,
        user_metadata: dict[str, dict[str, bytes]] = ...,
        system_metadata: dict[str, dict[str, bytes]] = ...,
    ) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

_MapHandler = Callable[[Datum], Awaitable[list[Message]]]

class _MapAsyncServer:
    def __init__(
        self,
        sock_file: str | None = ...,
        server_info_file: str | None = ...,
    ) -> None: ...
    def start(self, handler: _MapHandler) -> Awaitable[None]: ...
    def wait_ready(self, timeout: float = ...) -> Awaitable[None]: ...
    def stop(self) -> None: ...

class Mapper:
    def __call__(self, datum: Datum) -> Awaitable[list[Message]]: ...
    async def handler(self, datum: Datum) -> list[Message]: ...

class MapAsyncServer:
    def __init__(
        self,
        handler: _MapHandler | Mapper,
        *,
        sock_file: str | None = ...,
        server_info_file: str | None = ...,
    ) -> None: ...
    def run(self) -> None: ...
    async def serve(self) -> None: ...
    def stop(self) -> None: ...
    async def wait_ready(self, timeout: float = ...) -> None: ...
    async def __aenter__(self) -> MapAsyncServer: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

__all__ = [
    "Datum",
    "MapAsyncServer",
    "Mapper",
    "Message",
]
