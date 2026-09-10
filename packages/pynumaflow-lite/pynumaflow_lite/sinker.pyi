from __future__ import annotations

import datetime as _dt
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType

class Message:
    keys: list[str] | None
    value: bytes
    user_metadata: dict[str, dict[str, bytes]] | None

    def __init__(
        self,
        value: bytes,
        keys: list[str] | None = ...,
        user_metadata: dict[str, dict[str, bytes]] | None = ...,
    ) -> None: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...

class Response:
    id: str
    error: str | None

    @staticmethod
    def success(id: str) -> Response: ...
    @staticmethod
    def failure(id: str, error: str) -> Response: ...
    @staticmethod
    def fallback(id: str) -> Response: ...
    @staticmethod
    def serve(id: str, payload: bytes) -> Response: ...
    @staticmethod
    def on_success(id: str, message: Message | None = ...) -> Response: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...

class Datum:
    keys: list[str]
    value: bytes
    watermark: _dt.datetime
    event_time: _dt.datetime
    id: str
    headers: dict[str, str]
    user_metadata: dict[str, dict[str, bytes]]
    system_metadata: dict[str, dict[str, bytes]]

    def __init__(
        self,
        *,
        keys: list[str] = ...,
        value: bytes = ...,
        id: str = ...,
        event_time: _dt.datetime | None = ...,
        watermark: _dt.datetime | None = ...,
        headers: dict[str, str] = ...,
        user_metadata: dict[str, dict[str, bytes]] = ...,
        system_metadata: dict[str, dict[str, bytes]] = ...,
    ) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

_SinkHandler = Callable[[AsyncIterator[Datum]], Awaitable[list[Response]]]

class _SinkAsyncServer:
    def __init__(
        self,
        sock_file: str | None = ...,
        server_info_file: str | None = ...,
    ) -> None: ...
    def start(self, handler: _SinkHandler) -> Awaitable[None]: ...
    def wait_ready(self, timeout: float = ...) -> Awaitable[None]: ...
    def stop(self) -> None: ...

class Sinker:
    def __call__(self, datums: AsyncIterator[Datum]) -> Awaitable[list[Response]]: ...
    async def handler(self, datums: AsyncIterator[Datum]) -> list[Response]: ...

class SinkAsyncServer:
    def __init__(
        self,
        handler: _SinkHandler | Sinker,
        *,
        sock_file: str | None = ...,
        server_info_file: str | None = ...,
    ) -> None: ...
    def run(self) -> None: ...
    async def serve(self) -> None: ...
    def stop(self) -> None: ...
    async def wait_ready(self, timeout: float = ...) -> None: ...
    async def __aenter__(self) -> SinkAsyncServer: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

__all__ = [
    "Datum",
    "Message",
    "Response",
    "SinkAsyncServer",
    "Sinker",
]
