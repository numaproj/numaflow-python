from __future__ import annotations

import datetime as _dt
from collections.abc import AsyncIterator, Awaitable, Callable

class SystemMetadata:
    """System-generated metadata groups per message (read-only for sink)."""

    def __init__(self) -> None: ...
    def groups(self) -> list[str]:
        """Returns the groups of the system metadata."""
        ...

    def keys(self, group: str) -> list[str]:
        """Returns the keys of the system metadata for the given group."""
        ...

    def value(self, group: str, key: str) -> bytes:
        """Returns the value of the system metadata for the given group and key."""
        ...

    def __repr__(self) -> str: ...

class UserMetadata:
    """User-defined metadata groups per message (read-only for sink)."""

    def __init__(self) -> None: ...
    def groups(self) -> list[str]:
        """Returns the groups of the user metadata."""
        ...

    def keys(self, group: str) -> list[str]:
        """Returns the keys of the user metadata for the given group."""
        ...

    def value(self, group: str, key: str) -> bytes:
        """Returns the value of the user metadata for the given group and key."""
        ...

    def __repr__(self) -> str: ...

class KeyValueGroup:
    key_value: dict[str, bytes]

    def __init__(self, key_value: dict[str, bytes] | None = ...) -> None: ...
    @staticmethod
    def from_dict(key_value: dict[str, bytes]) -> KeyValueGroup: ...

class Message:
    keys: list[str] | None
    value: bytes
    user_metadata: dict[str, KeyValueGroup] | None

    def __init__(
        self,
        value: bytes,
        keys: list[str] | None = ...,
        user_metadata: dict[str, KeyValueGroup] | None = ...,
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
    def as_on_success(id: str, message: Message | None = ...) -> Response: ...

class Responses:
    def __init__(self) -> None: ...
    def append(self, response: Response) -> None: ...

class Datum:
    keys: list[str]
    value: bytes
    watermark: _dt.datetime
    eventtime: _dt.datetime
    id: str
    headers: dict[str, str]
    user_metadata: UserMetadata
    system_metadata: SystemMetadata

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class SinkAsyncServer:
    def __init__(
        self,
        sock_file: str | None = ...,
        info_file: str | None = ...,
    ) -> None: ...
    def start(
        self, py_func: Callable[[AsyncIterator[Datum]], Awaitable[Responses]]
    ) -> Awaitable[None]: ...
    def stop(self) -> None: ...

class Sinker:
    async def handler(self, datums: AsyncIterator[Datum]) -> Responses: ...

__all__ = [
    "Datum",
    "KeyValueGroup",
    "Message",
    "Response",
    "Responses",
    "SinkAsyncServer",
    "Sinker",
    "SystemMetadata",
    "UserMetadata",
]
