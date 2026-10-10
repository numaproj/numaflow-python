from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeAlias

from ._async_server import _AsyncServer
from .pynumaflow_lite import mapper as _mapper

Datum: TypeAlias = _mapper.Datum
Message: TypeAlias = _mapper.Message


class MapAsyncServer(_AsyncServer):
    _name = "map"

    def __init__(
        self,
        handler: Callable[[Datum], Awaitable[list[Message]]],
        *,
        sock_file: str | None = None,
        server_info_file: str | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        super().__init__(
            _mapper._MapAsyncServer(sock_file, server_info_file),
            handler,
            install_signal_handlers=install_signal_handlers,
        )
