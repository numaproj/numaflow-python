from __future__ import annotations

from typing import Optional, List, Dict, Callable, Awaitable, Any, AsyncIterator
import datetime as _dt


class KeyValueGroup:
    key_value: Dict[str, bytes]

    def __init__(self, key_value: Optional[Dict[str, bytes]] = ...) -> None: ...

    @staticmethod
    def from_dict(key_value: Dict[str, bytes]) -> KeyValueGroup: ...


class Message:
    keys: Optional[List[str]]
    value: bytes
    user_metadata: Optional[Dict[str, KeyValueGroup]]

    def __init__(
            self,
            value: bytes,
            keys: Optional[List[str]] = ...,
            user_metadata: Optional[Dict[str, KeyValueGroup]] = ...,
    ) -> None: ...


class Response:
    id: str

    @staticmethod
    def as_success(id: str) -> Response: ...

    @staticmethod
    def as_failure(id: str, err_msg: str) -> Response: ...

    @staticmethod
    def as_fallback(id: str) -> Response: ...

    @staticmethod
    def as_serve(id: str, payload: bytes) -> Response: ...

    @staticmethod
    def as_on_success(id: str, message: Optional[Message] = ...) -> Response: ...


class Responses:
    def __init__(self) -> None: ...

    def append(self, response: Response) -> None: ...


class Datum:
    keys: List[str]
    value: bytes
    watermark: _dt.datetime
    eventtime: _dt.datetime
    id: str
    headers: Dict[str, str]

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class SinkAsyncServer:
    def __init__(
            self,
            sock_file: str | None = ...,
            info_file: str | None = ...,
    ) -> None: ...

    def start(self, py_func: Callable[..., Any]) -> Awaitable[None]: ...

    def stop(self) -> None: ...


class Sinker:
    async def handler(self, datums: AsyncIterator[Datum]) -> Responses: ...


__all__ = [
    "KeyValueGroup",
    "Message",
    "Response",
    "Responses",
    "Datum",
    "SinkAsyncServer",
    "Sinker",
]

