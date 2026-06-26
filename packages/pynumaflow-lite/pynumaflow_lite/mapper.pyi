from __future__ import annotations

import datetime as _dt
from collections.abc import Awaitable, Callable

# Re-export the Python ABC for user convenience and typing
from ._map_dtypes import Mapper as Mapper

class SystemMetadata:
    """System-generated metadata groups per message (read-only)."""

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
    """User-defined metadata groups per message (read-write)."""

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

    def create_group(self, group: str) -> None:
        """Creates a new group in the user metadata."""
        ...

    def add_kv(self, group: str, key: str, value: bytes) -> None:
        """Adds a key-value pair to the user metadata."""
        ...

    def remove_key(self, group: str, key: str) -> None:
        """Removes a key from a group in the user metadata."""
        ...

    def remove_group(self, group: str) -> None:
        """Removes a group from the user metadata."""
        ...

    def __repr__(self) -> str: ...

class Messages:
    def __init__(self) -> None: ...
    def append(self, message: Message) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class Message:
    keys: list[str] | None
    value: bytes
    tags: list[str] | None
    user_metadata: UserMetadata | None

    def __init__(
        self,
        value: bytes,
        keys: list[str] | None = ...,
        tags: list[str] | None = ...,
        user_metadata: UserMetadata | None = ...,
    ) -> None: ...
    @staticmethod
    def message_to_drop() -> Message: ...

class Datum:
    # Read-only attributes provided by the extension
    keys: list[str]
    value: bytes
    watermark: _dt.datetime
    eventtime: _dt.datetime
    headers: dict[str, str]
    user_metadata: UserMetadata
    system_metadata: SystemMetadata

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class MapAsyncServer:
    def __init__(
        self,
        sock_file: str | None = ...,
        info_file: str | None = ...,
    ) -> None: ...
    def start(
        self, py_func: Callable[[list[str], Datum], Awaitable[Messages]]
    ) -> Awaitable[None]: ...
    def stop(self) -> None: ...

# Simple utility function exposed by the extension

__all__ = [
    "Datum",
    "MapAsyncServer",
    "Mapper",
    "Message",
    "Messages",
    "SystemMetadata",
    "UserMetadata",
]
