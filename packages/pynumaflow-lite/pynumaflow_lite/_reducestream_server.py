from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable
from typing import TypeAlias

from ._async_server import _AsyncServer
from ._reducestreamer_dtypes import ReduceStreamer
from .pynumaflow_lite import reducestreamer as _reducestreamer

Datum: TypeAlias = _reducestreamer.Datum
Message: TypeAlias = _reducestreamer.Message
Metadata: TypeAlias = _reducestreamer.Metadata

ReduceStreamHandler: TypeAlias = Callable[[list[str], AsyncIterator[Datum], Metadata], AsyncIterator[Message]]


class ReduceStreamAsyncServer(_AsyncServer):
    """Reduce stream server.

    `handler` is an async generator function, or a zero-argument factory that
    returns a handler, such as a `ReduceStreamer` subclass or
    `functools.partial(MyReduceStreamer, x=1)`. The server calls a factory once
    for each window and key set, so each call gets its own state.
    """

    _name = "reducestream"

    def __init__(
        self,
        handler: ReduceStreamHandler | Callable[[], ReduceStreamHandler],
        *,
        sock_file: str | None = None,
        server_info_file: str | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        if isinstance(handler, ReduceStreamer):
            raise TypeError(
                "pass the ReduceStreamer class or a factory, not an instance: "
                "the server creates a new instance for each window"
            )
        factory = (lambda: handler) if inspect.isasyncgenfunction(handler) else handler
        super().__init__(
            _reducestreamer._ReduceStreamAsyncServer(sock_file, server_info_file),
            factory,
            install_signal_handlers=install_signal_handlers,
        )
