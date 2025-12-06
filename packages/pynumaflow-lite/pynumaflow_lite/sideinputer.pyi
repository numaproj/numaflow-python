from __future__ import annotations

from typing import Callable, Awaitable, Any

# Re-export the Python ABC for user convenience and typing
from ._sideinput_dtypes import SideInput as SideInput


class Response:
    """Response from the side input retrieve handler."""
    
    value: bytes
    broadcast: bool

    @staticmethod
    def broadcast_message(value: bytes) -> Response:
        """Create a response that broadcasts the given value."""
        ...

    @staticmethod
    def no_broadcast_message() -> Response:
        """Create a response that does not broadcast any value."""
        ...

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class SideInputAsyncServer:
    """Async SideInput Server that can be started from Python."""
    
    def __init__(
            self,
            sock_file: str | None = ...,
            info_file: str | None = ...,
    ) -> None: ...

    def start(self, py_sideinput: SideInput) -> Awaitable[None]: ...

    def stop(self) -> None: ...


__all__ = [
    "Response",
    "SideInputAsyncServer",
    "SideInput",
]

