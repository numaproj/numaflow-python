from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeAlias

from ._async_server import _AsyncServer
from ._reduce_dtypes import Reducer
from .pynumaflow_lite import reducer as _reducer

Datum: TypeAlias = _reducer.Datum
Message: TypeAlias = _reducer.Message
Metadata: TypeAlias = _reducer.Metadata

ReduceHandler: TypeAlias = Callable[[list[str], AsyncIterator[Datum], Metadata], Awaitable[list[Message]]]


class ReduceAsyncServer(_AsyncServer):
    """Reduce server.

    `handler` is an async function, or a zero-argument factory that returns a
    handler, such as a `Reducer` subclass or `functools.partial(MyReducer, x=1)`.
    The server calls a factory once for each window and key set, so each call
    gets its own state.
    """

    _name = "reduce"

    def __init__(
        self,
        handler: ReduceHandler | Callable[[], ReduceHandler],
        *,
        sock_file: str | None = None,
        server_info_file: str | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        if isinstance(handler, Reducer):
            raise TypeError(
                "pass the Reducer class or a factory, not an instance: "
                "the server creates a new instance for each window"
            )
        factory = (lambda: handler) if inspect.iscoroutinefunction(handler) else handler
        super().__init__(
            _reducer._ReduceAsyncServer(sock_file, server_info_file),
            factory,
            install_signal_handlers=install_signal_handlers,
        )
