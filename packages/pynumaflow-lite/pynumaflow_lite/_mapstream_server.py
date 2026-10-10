from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import TypeAlias

from ._async_server import _AsyncServer
from .pynumaflow_lite import mapstreamer as _mapstreamer

Datum: TypeAlias = _mapstreamer.Datum
Message: TypeAlias = _mapstreamer.Message


class MapStreamAsyncServer(_AsyncServer):
    _name = "mapstream"

    def __init__(
        self,
        handler: Callable[[Datum], AsyncIterator[Message]],
        *,
        sock_file: str | None = None,
        server_info_file: str | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        super().__init__(
            _mapstreamer._MapStreamAsyncServer(sock_file, server_info_file),
            handler,
            install_signal_handlers=install_signal_handlers,
        )
