from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeAlias

from ._async_server import _AsyncServer
from .pynumaflow_lite import sourcetransformer as _sourcetransformer

Datum: TypeAlias = _sourcetransformer.Datum
Message: TypeAlias = _sourcetransformer.Message


class SourceTransformAsyncServer(_AsyncServer):
    _name = "sourcetransform"

    def __init__(
        self,
        handler: Callable[[Datum], Awaitable[list[Message]]],
        *,
        sock_file: str | None = None,
        server_info_file: str | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        super().__init__(
            _sourcetransformer._SourceTransformAsyncServer(sock_file, server_info_file),
            handler,
            install_signal_handlers=install_signal_handlers,
        )
