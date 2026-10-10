from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Callable
from typing import TypeAlias

from ._async_server import _AsyncServer
from .pynumaflow_lite import batchmapper as _batchmapper

Datum: TypeAlias = _batchmapper.Datum
BatchResponse: TypeAlias = _batchmapper.BatchResponse


class BatchMapAsyncServer(_AsyncServer):
    _name = "batchmap"

    def __init__(
        self,
        handler: Callable[[AsyncIterable[Datum]], Awaitable[list[BatchResponse]]],
        *,
        sock_file: str | None = None,
        server_info_file: str | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        super().__init__(
            _batchmapper._BatchMapAsyncServer(sock_file, server_info_file),
            handler,
            install_signal_handlers=install_signal_handlers,
        )
