from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Protocol, Self

_SHUTDOWN_SIGNALS = (signal.SIGINT, signal.SIGTERM)


class _ServerCore(Protocol):
    """The Rust server object that a `_AsyncServer` drives."""

    def start(self, handler: Callable[..., object]) -> Awaitable[None]: ...
    def wait_ready(self) -> Awaitable[None]: ...
    def stop(self) -> None: ...


class _AsyncServer:
    """Lifecycle shared by the async UDF servers.

    A subclass sets `_name` for error messages, and gives its Rust core object
    and the user handler to `__init__`.
    """

    _name: str

    def __init__(
        self,
        core: _ServerCore,
        handler: Callable[..., object],
        *,
        install_signal_handlers: bool,
    ) -> None:
        self._core = core
        self._handler = handler
        self._install_signal_handlers = install_signal_handlers
        self._task: asyncio.Task[None] | None = None
        self._serving = False
        self._installed_signals: list[signal.Signals] = []

    async def serve(self) -> None:
        """Run the server until it stops.

        This is the entrypoint for an application that already runs an event
        loop. It returns when a shutdown signal arrives or when `stop()` runs.
        """
        await self._serve(install_signal_handlers=self._install_signal_handlers)

    async def _serve(self, *, install_signal_handlers: bool) -> None:
        if self._serving:
            raise RuntimeError(f"{self._name} server is already serving")
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

    async def wait_ready(self) -> None:
        """Wait until the server answers the Numaflow readiness probe.

        There is no time limit. Use `asyncio.timeout()` to set one.
        """
        await self._core.wait_ready()

    async def wait_for_termination(self) -> None:
        """Wait until the background server task ends.

        Use this inside an `async with` block. It raises the handler error if
        the server task failed.
        """
        if self._task is None:
            raise RuntimeError(f"{self._name} server is not serving")
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

    async def __aenter__(self) -> Self:
        """Start the server in a background task and wait until it is ready.

        This form is for tests and for code that must run other work next to
        the server. It never installs signal handlers. There is no time limit:
        use `asyncio.timeout()` to set one. If the server stops before it is
        ready, this raises the server error at once.
        """
        if self._task is not None and not self._task.done():
            raise RuntimeError(f"{self._name} server is already serving")

        task = self._task = asyncio.create_task(self._serve(install_signal_handlers=False))
        ready = asyncio.create_task(self.wait_ready())
        try:
            await asyncio.wait((task, ready), return_when=asyncio.FIRST_COMPLETED)
            if task.done():
                task.result()
                raise RuntimeError(f"{self._name} server stopped before it was ready")
            ready.result()
        except BaseException:
            ready.cancel()
            await asyncio.wait((ready,))
            self.stop()
            self._task = None
            # Surface the server error, if there is one. It explains the
            # failure better than a cancellation or a timeout does.
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
