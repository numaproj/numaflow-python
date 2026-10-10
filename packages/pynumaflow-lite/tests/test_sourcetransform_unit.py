import asyncio
import os
import subprocess
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pynumaflow_lite import sourcetransformer

CARGO_ROOT = Path(__file__).resolve().parent.parent

EVENT_TIME = datetime(2023, 6, 1, tzinfo=UTC)


async def _unit_handler(datum: sourcetransformer.Datum) -> list[sourcetransformer.Message]:
    assert datum.user_metadata["custom"]["version"] == b"1.0.0"
    return [sourcetransformer.Message(datum.value, datum.event_time, keys=datum.keys, tags=["seen"])]


def test_sourcetransform_handler_can_be_unit_tested_without_server():
    datum = sourcetransformer.Datum(
        keys=["key"],
        value=b"value",
        event_time=EVENT_TIME,
        user_metadata={"custom": {"version": b"1.0.0"}},
    )

    messages = asyncio.run(_unit_handler(datum))

    assert messages == [sourcetransformer.Message(b"value", EVENT_TIME, keys=["key"], tags=["seen"])]


def test_message_attributes_and_repr():
    message = sourcetransformer.Message(
        b"value",
        EVENT_TIME,
        keys=["key"],
        user_metadata={"group": {"k": b"v"}},
    )

    assert message.value == b"value"
    assert message.event_time == EVENT_TIME
    assert message.keys == ["key"]
    assert message.tags is None
    assert message.user_metadata == {"group": {"k": b"v"}}
    assert repr(message).startswith("Message(value=b'value', ")
    assert sourcetransformer.Message.to_drop(EVENT_TIME).event_time == EVENT_TIME


async def _run_sourcetransform_client(sock_path: Path) -> subprocess.CompletedProcess[str]:
    return await asyncio.to_thread(
        subprocess.run,
        ["cargo", "run", "--quiet", "--bin", "test_sourcetransform", "--", str(sock_path)],
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
    server = sourcetransformer.SourceTransformAsyncServer(
        handler,
        sock_file=str(sock_path),
        server_info_file=str(server_info_path),
    )

    try:
        async with server:
            client = await _run_sourcetransform_client(sock_path)
            if clients is not None:
                clients.append(client)
    finally:
        for path in (sock_path, server_info_path):
            with suppress(FileNotFoundError):
                os.unlink(path)


def test_sourcetransform_server_propagates_handler_exception(tmp_path: Path):
    async def handler(datum: sourcetransformer.Datum) -> list[sourcetransformer.Message]:
        raise RuntimeError("transform exploded")

    clients: list[subprocess.CompletedProcess[str]] = []
    with pytest.raises(RuntimeError, match="transform exploded"):
        asyncio.run(_exercise_server(tmp_path, handler, clients))

    # The client must get a gRPC error, not an empty result. Do not check for the
    # handler text: numaflow-rs keeps only the first panic of the process, and an
    # earlier test in the same pytest process can own it.
    [client] = clients
    assert client.returncode != 0
    assert "UDF_EXECUTION_ERROR" in client.stderr


def test_sourcetransform_server_rejects_non_list_response(tmp_path: Path):
    async def handler(datum: sourcetransformer.Datum) -> str:
        return "not messages"

    with pytest.raises(TypeError, match=r"sourcetransform handler must return list\[Message\]"):
        asyncio.run(_exercise_server(tmp_path, handler))


def test_sourcetransform_server_rejects_sync_handler(tmp_path: Path):
    def handler(datum: sourcetransformer.Datum) -> list[sourcetransformer.Message]:
        return []

    with pytest.raises(TypeError, match="sourcetransform handler must be an async function"):
        asyncio.run(_exercise_server(tmp_path, handler))
