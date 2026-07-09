import asyncio
import os
import subprocess
import uuid
from contextlib import suppress
from pathlib import Path

import pytest

from pynumaflow_lite import mapper

CARGO_ROOT = Path(__file__).resolve().parent.parent


async def _unit_handler(datum: mapper.Datum) -> list[mapper.Message]:
    assert datum.value == b"value"
    assert datum.user_metadata["custom"]["version"] == b"1.0.0"
    return [mapper.Message(datum.value, keys=datum.keys)]


def test_map_handler_can_be_unit_tested_without_server():
    datum = mapper.Datum(
        keys=["key"],
        value=b"value",
        user_metadata={"custom": {"version": b"1.0.0"}},
    )
    messages = asyncio.run(_unit_handler(datum))

    assert messages == [mapper.Message(b"value", keys=["key"])]


def test_to_drop_message():
    msg = mapper.Message.to_drop()

    assert msg.value == b""
    assert msg.tags is not None
    assert len(msg.tags) == 1


async def _run_map_client(sock_path: Path) -> subprocess.CompletedProcess[str]:
    return await asyncio.to_thread(
        subprocess.run,
        ["cargo", "run", "--quiet", "--bin", "test_map", "--", str(sock_path)],
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
    server = mapper.MapAsyncServer(
        handler,
        sock_file=str(sock_path),
        server_info_file=str(server_info_path),
    )

    try:
        async with server:
            await _run_map_client(sock_path)
    finally:
        for path in (sock_path, server_info_path):
            with suppress(FileNotFoundError):
                os.unlink(path)


def test_map_server_propagates_handler_exception(tmp_path: Path):
    async def handler(datum: mapper.Datum) -> list[mapper.Message]:
        raise RuntimeError("map exploded")

    with pytest.raises(RuntimeError, match="map exploded"):
        asyncio.run(_exercise_server(tmp_path, handler))


def test_map_server_rejects_non_list_response(tmp_path: Path):
    async def handler(datum: mapper.Datum) -> str:
        return "not messages"

    with pytest.raises(TypeError, match=r"map handler must return list\[Message\]"):
        asyncio.run(_exercise_server(tmp_path, handler))


def test_map_server_rejects_sync_handler(tmp_path: Path):
    def handler(datum: mapper.Datum) -> list[mapper.Message]:
        return []

    with pytest.raises(TypeError, match="map handler must be an async function"):
        asyncio.run(_exercise_server(tmp_path, handler))
