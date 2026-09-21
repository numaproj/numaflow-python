import asyncio
import os
import subprocess
import uuid
from collections.abc import AsyncIterator
from contextlib import suppress
from pathlib import Path

import pytest

from pynumaflow_lite import sinker

CARGO_ROOT = Path(__file__).resolve().parent.parent


async def _single_datum() -> AsyncIterator[sinker.Datum]:
    yield sinker.Datum(
        keys=["key"],
        value=b"value",
        id="1",
        user_metadata={"custom": {"version": b"1.0.0"}},
    )


async def _unit_handler(datums: AsyncIterator[sinker.Datum]) -> list[sinker.Response]:
    responses = []
    async for datum in datums:
        assert datum.value == b"value"
        assert datum.user_metadata["custom"]["version"] == b"1.0.0"
        responses.append(sinker.Response.success(datum.id))
    return responses


def test_sink_handler_can_be_unit_tested_without_server():
    responses = asyncio.run(_unit_handler(_single_datum()))

    assert responses == [sinker.Response.success("1")]


async def _run_sink_client(sock_path: Path) -> subprocess.CompletedProcess[str]:
    return await asyncio.to_thread(
        subprocess.run,
        ["cargo", "run", "--quiet", "--bin", "test_sink", "--", str(sock_path)],
        cwd=CARGO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


async def _exercise_server(tmp_path: Path, handler) -> None:
    path_id = f"{tmp_path.name[:12]}-{uuid.uuid4().hex[:12]}"
    sock_path = Path(f"/tmp/pnl-{path_id}.sock")
    server_info_path = Path(f"/tmp/pnl-{path_id}.info")
    server = sinker.SinkAsyncServer(
        handler,
        sock_file=str(sock_path),
        server_info_file=str(server_info_path),
    )

    try:
        async with server:
            await _run_sink_client(sock_path)
    finally:
        for path in (sock_path, server_info_path):
            with suppress(FileNotFoundError):
                os.unlink(path)


def test_sink_server_propagates_handler_exception(tmp_path: Path):
    async def handler(datums: AsyncIterator[sinker.Datum]) -> list[sinker.Response]:
        async for _datum in datums:
            raise RuntimeError("sink exploded")
        return []

    with pytest.raises(RuntimeError, match="sink exploded"):
        asyncio.run(_exercise_server(tmp_path, handler))


def test_sink_server_rejects_non_list_response(tmp_path: Path):
    async def handler(datums: AsyncIterator[sinker.Datum]) -> str:
        async for _datum in datums:
            return "not responses"
        return "not responses"

    with pytest.raises(TypeError, match=r"sink handler must return list\[Response\]"):
        asyncio.run(_exercise_server(tmp_path, handler))


def test_sink_server_rejects_sync_handler(tmp_path: Path):
    def handler(datums: AsyncIterator[sinker.Datum]) -> list[sinker.Response]:
        return []

    with pytest.raises(TypeError, match="sink handler must be an async function"):
        asyncio.run(_exercise_server(tmp_path, handler))
