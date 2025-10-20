from __future__ import annotations

from typing import Optional, List, Dict, Callable, Awaitable, Any, AsyncIterator
import datetime as _dt


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
    id: str
    headers: Dict[str, str]

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class BatchResponse:
    id: str

    def __init__(self, id: str) -> None: ...

    @staticmethod
    def from_id(id: str) -> BatchResponse: ...

    def append(self, message: Message) -> None: ...


class BatchResponses:
    def __init__(self) -> None: ...

    def append(self, response: BatchResponse) -> None: ...


class BatchMapAsyncServer:
    def __init__(
            self,
            sock_file: str | None = ...,
            info_file: str | None = ...,
    ) -> None: ...

    def start(self, py_func: Callable[..., Any]) -> Awaitable[None]: ...

    def stop(self) -> None: ...


class BatchMapper:
    async def handler(self, batch: AsyncIterator[Datum]) -> BatchResponses: ...


__all__ = [
    "Message",
    "Datum",
    "BatchResponse",
    "BatchResponses",
    "BatchMapAsyncServer",
    "BatchMapper",
]
