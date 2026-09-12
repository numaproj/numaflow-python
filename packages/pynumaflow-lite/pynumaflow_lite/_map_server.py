from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import Awaitable, Callable
from types import TracebackType

from .pynumaflow_lite import mapper as _mapper

Datum = _mapper.Datum
Message = _mapper.Message

_SHUTDOWN_SIGNALS = (signal.SIGINT, signal.SIGTERM)


class MapAsyncServer:
    def __init__(
        self,
        handler: Callable[[Datum], Awaitable[list[Message]]],
        *,
        sock_file: str | None = None,
        server_info_file: str | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        self._core = _mapper._MapAsyncServer(sock_file, server_info_file)
        self._handler = handler
        self._install_signal_handlers = install_signal_handlers
        self._task: asyncio.Task[None] | None = None
        self._serving = False
        self._installed_signals: list[signal.Signals] = []

    async def serve(self) -> None:
        """Run the map server until it stops.

        This is the entrypoint for an application that already runs an event
        loop. It returns when a shutdown signal arrives or when `stop()` runs.
        """
        await self._serve(install_signal_handlers=self._install_signal_handlers)

    async def _serve(self, *, install_signal_handlers: bool) -> None:
        if self._serving:
            raise RuntimeError("map server is already serving")
        self._serving = True
        try:
            if install_signal_handlers:
                self._add_signal_handlers()
            await self._core.start(self._handler)
        finally:
            self._remove_signal_handlers()
            self._serving = False

    def stop(self) -> None:
        self._core.stop()

    async def wait_ready(self, timeout: float = 30.0) -> None:
        await self._core.wait_ready(timeout)

    async def wait_for_termination(self) -> None:
        """Wait until the background server task ends.

        Use this inside an `async with` block. It raises the handler error if
        the server task failed.
        """
        if self._task is None:
            raise RuntimeError("map server is not serving")
        await asyncio.shield(self._task)

    def _add_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in _SHUTDOWN_SIGNALS:
            try:
                loop.add_signal_handler(sig, self.stop)
            except (NotImplementedError, RuntimeError, OSError):
                continue
            self._installed_signals.append(sig)

    def _remove_signal_handlers(self) -> None:
        if not self._installed_signals:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._installed_signals.clear()
            return
        for sig in self._installed_signals:
            with contextlib.suppress(NotImplementedError, OSError):
                loop.remove_signal_handler(sig)
        self._installed_signals.clear()

    async def __aenter__(self) -> MapAsyncServer:
        """Start the server in a background task and wait until it is ready.

        This form is for tests and for code that must run other work next to
        the server. It never installs signal handlers.
        """
        if self._task is not None and not self._task.done():
            raise RuntimeError("map server is already serving")

        self._task = asyncio.create_task(self._serve(install_signal_handlers=False))
        try:
            await self.wait_ready()
        except BaseException:
            self.stop()
            task, self._task = self._task, None
            if task is not None:
                # Surface the server error, if there is one. It explains the
                # failure better than the `wait_ready` error does.
                await task
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()
        if self._task is not None:
            try:
                await self._task
            finally:
                self._task = None

    def run(self) -> None:
        """Run the map server in a new event loop until it stops."""
        try:
            asyncio.run(self.serve())
        except KeyboardInterrupt:
            self.stop()
