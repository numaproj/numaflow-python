import datetime as _dt
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType
from typing import Self, TypeAlias

# Re-export the Python ABC for user convenience and typing
from ._reduce_dtypes import Reducer as Reducer

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
    headers: dict[str, str]

    def __init__(
        self,
        *,
        keys: list[str] = ...,
        value: bytes = ...,
        event_time: _dt.datetime | None = ...,
        watermark: _dt.datetime | None = ...,
        headers: dict[str, str] = ...,
    ) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class IntervalWindow:
    start: _dt.datetime
    end: _dt.datetime

    def __init__(self, start: _dt.datetime, end: _dt.datetime) -> None: ...

class Metadata:
    interval_window: IntervalWindow

    def __init__(self, *, interval_window: IntervalWindow) -> None: ...

_ReduceHandler: TypeAlias = Callable[[list[str], AsyncIterator[Datum], Metadata], Awaitable[list[Message]]]

class _ReduceAsyncServer:
    def __init__(
        self,
        sock_file: str | None = ...,
        server_info_file: str | None = ...,
    ) -> None: ...
    def start(
        self,
        factory: Callable[[], _ReduceHandler],
    ) -> Awaitable[None]: ...
    def wait_ready(self) -> Awaitable[None]: ...
    def stop(self) -> None: ...

class ReduceAsyncServer:
    def __init__(
        self,
        handler: _ReduceHandler | Callable[[], _ReduceHandler],
        *,
        sock_file: str | None = ...,
        server_info_file: str | None = ...,
        install_signal_handlers: bool = ...,
    ) -> None: ...
    async def serve(self) -> None: ...
    def stop(self) -> None: ...
    async def wait_ready(self) -> None: ...
    async def wait_for_termination(self) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

__all__ = [
    "Datum",
    "IntervalWindow",
    "Message",
    "Metadata",
    "ReduceAsyncServer",
    "Reducer",
]
