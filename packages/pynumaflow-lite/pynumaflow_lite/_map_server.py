from __future__ import annotations

import asyncio
import signal
from types import TracebackType
from typing import Any

from .pynumaflow_lite import mapper as _mapper


class MapAsyncServer:
    def __init__(
        self,
        handler: Any,
        *,
        sock_file: str | None = None,
        server_info_file: str | None = None,
    ) -> None:
        self._core = _mapper._MapAsyncServer(sock_file, server_info_file)
        self._handler = handler
        self._task: asyncio.Task[None] | None = None
        self._serving = False

    async def serve(self) -> None:
        if self._serving:
            raise RuntimeError("map server is already serving")
        self._serving = True
        try:
            await self._core.start(self._handler)
        finally:
            self._serving = False

    def stop(self) -> None:
        self._core.stop()

    async def wait_ready(self, timeout: float = 30.0) -> None:
        await self._core.wait_ready(timeout)

    async def __aenter__(self) -> MapAsyncServer:
        if self._task is not None and not self._task.done():
            raise RuntimeError("map server is already serving")

        self._task = asyncio.create_task(self.serve())
        try:
            await self.wait_ready()
        except asyncio.CancelledError:
            self.stop()
            if self._task is not None:
                await self._task
            raise
        except Exception:
            self.stop()
            if self._task is not None:
                await self._task
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

    async def _main(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, self.stop)
            loop.add_signal_handler(signal.SIGTERM, self.stop)
        except (NotImplementedError, RuntimeError):
            pass

        await self.serve()

    def run(self) -> None:
        try:
            asyncio.run(self._main())
        except KeyboardInterrupt:
            self.stop()
