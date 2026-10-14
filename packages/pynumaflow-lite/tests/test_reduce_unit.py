import asyncio
import functools
import os
import subprocess
import uuid
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pynumaflow_lite import reducer

CARGO_ROOT = Path(__file__).resolve().parent.parent

WINDOW = reducer.IntervalWindow(datetime(2023, 6, 1, tzinfo=UTC), datetime(2023, 6, 1, 0, 1, tzinfo=UTC))


class Counter(reducer.Reducer):
    def __init__(self, initial: int = 0) -> None:
        self.counter = initial

    async def handler(
        self, keys: list[str], datums: AsyncIterable[reducer.Datum], md: reducer.Metadata
    ) -> list[reducer.Message]:
        async for _ in datums:
            self.counter += 1
        return [reducer.Message(f"counter:{self.counter} ".encode(), keys=keys)]


async def _datums(count: int) -> AsyncIterator[reducer.Datum]:
    for _ in range(count):
        yield reducer.Datum(keys=["key"], value=b"value")


def test_reduce_handler_can_be_unit_tested_without_server():
    md = reducer.Metadata(interval_window=WINDOW)

    messages = asyncio.run(Counter(initial=10).handler(["key"], _datums(2), md))

    assert messages == [reducer.Message(b"counter:12 ", keys=["key"])]
    assert md.interval_window.start == WINDOW.start


def test_reduce_server_rejects_reducer_instance():
    with pytest.raises(TypeError, match="not an instance"):
        reducer.ReduceAsyncServer(Counter())


async def _run_reduce_client(sock_path: Path) -> subprocess.CompletedProcess[str]:
    return await asyncio.to_thread(
        subprocess.run,
        ["cargo", "run", "--quiet", "--bin", "test_reduce", "--", str(sock_path)],
        cwd=CARGO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


async def _exercise_server(
    tmp_path: Path,
    handler,
    clients: list[subprocess.CompletedProcess[str]] | None = None,
) -> None:
    path_id = f"{tmp_path.name[:12]}-{uuid.uuid4().hex[:12]}"
    sock_path = Path(f"/tmp/pnl-{path_id}.sock")
    server_info_path = Path(f"/tmp/pnl-{path_id}.info")
    server = reducer.ReduceAsyncServer(
        handler,
        sock_file=str(sock_path),
        server_info_file=str(server_info_path),
    )

    try:
        async with server:
            client = await _run_reduce_client(sock_path)
            if clients is not None:
                clients.append(client)
    finally:
        for path in (sock_path, server_info_path):
            with suppress(FileNotFoundError):
                os.unlink(path)


def test_reduce_server_creates_reducer_from_factory(tmp_path: Path):
    # The test client sends one datum and checks for "counter:1 ".
    clients: list[subprocess.CompletedProcess[str]] = []
    asyncio.run(_exercise_server(tmp_path, functools.partial(Counter, initial=0), clients))

    [client] = clients
    assert client.returncode == 0, client.stderr


def test_reduce_server_propagates_handler_exception(tmp_path: Path):
    async def handler(
        keys: list[str], datums: AsyncIterable[reducer.Datum], md: reducer.Metadata
    ) -> list[reducer.Message]:
        async for _datum in datums:
            raise RuntimeError("reduce exploded")
        return []

    clients: list[subprocess.CompletedProcess[str]] = []
    with pytest.raises(RuntimeError, match="reduce exploded"):
        asyncio.run(_exercise_server(tmp_path, handler, clients))

    # The client must get a gRPC error, not an empty result. Do not check for the
    # handler text: numaflow-rs keeps only the first panic of the process.
    [client] = clients
    assert client.returncode != 0
    assert "UDF_EXECUTION_ERROR" in client.stderr


def test_reduce_server_propagates_factory_exception(tmp_path: Path):
    def factory():
        raise RuntimeError("factory exploded")

    with pytest.raises(RuntimeError, match="factory exploded"):
        asyncio.run(_exercise_server(tmp_path, factory))


def test_reduce_server_rejects_non_list_response(tmp_path: Path):
    async def handler(keys: list[str], datums: AsyncIterable[reducer.Datum], md: reducer.Metadata) -> str:
        async for _datum in datums:
            pass
        return "not messages"

    with pytest.raises(TypeError, match=r"reduce handler must return list\[Message\]"):
        asyncio.run(_exercise_server(tmp_path, handler))


def test_reduce_server_rejects_sync_handler(tmp_path: Path):
    def factory():
        def handler(keys, datums, md):
            return []

        return handler

    with pytest.raises(TypeError, match="reduce handler must be an async function"):
        asyncio.run(_exercise_server(tmp_path, factory))


def test_reduce_server_wait_ready_obeys_asyncio_timeout(tmp_path: Path):
    server = reducer.ReduceAsyncServer(Counter, sock_file=str(tmp_path / "nobody-listens.sock"))

    async def wait() -> None:
        async with asyncio.timeout(0.2):
            await server.wait_ready()

    with pytest.raises(TimeoutError):
        asyncio.run(wait())
