from __future__ import annotations

from ._async_server import _AsyncServer
from ._source_dtypes import Sourcer
from .pynumaflow_lite import sourcer as _sourcer


class SourceAsyncServer(_AsyncServer):
    _name = "source"

    def __init__(
        self,
        handler: Sourcer,
        *,
        sock_file: str | None = None,
        server_info_file: str | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        if not isinstance(handler, Sourcer):
            raise TypeError(f"source handler must be a Sourcer instance, got {type(handler).__name__}")
        super().__init__(
            _sourcer._SourceAsyncServer(sock_file, server_info_file),
            handler,
            install_signal_handlers=install_signal_handlers,
        )
