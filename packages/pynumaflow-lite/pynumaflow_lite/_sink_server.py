from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeAlias

from ._async_server import _AsyncServer
from .pynumaflow_lite import sinker as _sinker

Datum: TypeAlias = _sinker.Datum
Response: TypeAlias = _sinker.Response


class SinkAsyncServer(_AsyncServer):
    _name = "sink"

    def __init__(
        self,
        handler: Callable[[AsyncIterator[Datum]], Awaitable[list[Response]]],
        *,
        sock_file: str | None = None,
        server_info_file: str | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        super().__init__(
            _sinker._SinkAsyncServer(sock_file, server_info_file),
            handler,
            install_signal_handlers=install_signal_handlers,
        )
