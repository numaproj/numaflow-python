import asyncio
import os
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from pynumaflow_lite import sourcer

CARGO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent / "examples"))

from source_simple import SimpleSource  # noqa: E402

EVENT_TIME = datetime(2023, 6, 1, tzinfo=UTC)


async def _read_all(source: sourcer.Sourcer, request: sourcer.ReadRequest) -> list[sourcer.Message]:
    return [message async for message in source.read(request)]


def test_sourcer_can_be_unit_tested_without_server():
    async def scenario() -> None:
        source = SimpleSource()

        messages = await _read_all(source, sourcer.ReadRequest(3))
        offsets = [message.offset for message in messages]
        assert offsets == [sourcer.Offset(b"0"), sourcer.Offset(b"1"), sourcer.Offset(b"2")]
        assert messages[0].user_metadata["source_info"]["message_id"] == b"0"

        await source.ack(offsets[:2])
        await source.nack([sourcer.NackOffset(offsets[2])])
        assert source.unacked == set()
        assert await source.pending() == 1

        [redelivered] = await _read_all(source, sourcer.ReadRequest(1))
        assert redelivered.offset == offsets[2]
        assert await source.active_partitions() == [0]
        assert await source.total_partitions() is None

    asyncio.run(scenario())


def test_offset_is_hashable_value_object():
    offset = sourcer.Offset(b"7", partition_id=2)

    assert offset == sourcer.Offset(b"7", 2)
    assert offset != sourcer.Offset(b"7", 3)
    assert {offset, sourcer.Offset(b"7", 2)} == {offset}
    assert offset.value == b"7"
    assert offset.partition_id == 2
    assert repr(offset) == "Offset(value=b'7', partition_id=2)"
    with pytest.raises(AttributeError):
        offset.value = b"8"  # type: ignore[misc]


def test_message_attributes_and_repr():
    message = sourcer.Message(b"value", sourcer.Offset(b"1"), EVENT_TIME, keys=["key"])

    assert message.value == b"value"
    assert message.offset == sourcer.Offset(b"1", 0)
    assert message.event_time == EVENT_TIME
    assert message.keys == ["key"]
    assert message.headers == {}
    assert message.user_metadata == {}
    assert message == sourcer.Message(b"value", sourcer.Offset(b"1"), EVENT_TIME, keys=["key"])
    assert repr(message).startswith("Message(value=b'value', offset=Offset(value=b'1', partition_id=0), ")


def test_read_request_timeout_is_timedelta():
    request = sourcer.ReadRequest(5)

    assert request.num_records == 5
    assert request.timeout == timedelta(seconds=1)
    assert sourcer.ReadRequest(5, timedelta(milliseconds=250)).timeout == timedelta(milliseconds=250)
    assert repr(request) == "ReadRequest(num_records=5, timeout=datetime.timedelta(seconds=1))"


def test_nack_offset_attributes():
    options = sourcer.NackOptions(delay=100, reason="retry")
    nack_offset = sourcer.NackOffset(sourcer.Offset(b"1"), options)

    assert nack_offset.offset == sourcer.Offset(b"1")
    assert nack_offset.nack_options == options
    assert sourcer.NackOffset(sourcer.Offset(b"1")).nack_options is None


def test_sourcer_requires_read_ack_nack_and_pending():
    class Incomplete(sourcer.Sourcer):
        pass

    with pytest.raises(TypeError) as excinfo:
        Incomplete()  # type: ignore[abstract]
    for method in ("read", "ack", "nack", "pending"):
        assert method in str(excinfo.value)


def test_server_rejects_non_sourcer():
    with pytest.raises(TypeError, match="source handler must be a Sourcer instance"):
        sourcer.SourceAsyncServer(object())  # type: ignore[arg-type]


async def _run_source_client(sock_path: Path) -> subprocess.CompletedProcess[str]:
    return await asyncio.to_thread(
        subprocess.run,
        ["cargo", "run", "--quiet", "--bin", "test_source", "--", str(sock_path)],
        cwd=CARGO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


async def _exercise_server(
    tmp_path: Path,
    source: sourcer.Sourcer,
    clients: list[subprocess.CompletedProcess[str]] | None = None,
) -> None:
    path_id = f"{tmp_path.name[:12]}-{uuid.uuid4().hex[:12]}"
    sock_path = Path(f"/tmp/pnl-{path_id}.sock")
    server_info_path = Path(f"/tmp/pnl-{path_id}.info")
    server = sourcer.SourceAsyncServer(
        source,
        sock_file=str(sock_path),
        server_info_file=str(server_info_path),
    )

    try:
        async with server:
            client = await _run_source_client(sock_path)
            if clients is not None:
                clients.append(client)
    finally:
        for path in (sock_path, server_info_path):
            with suppress(FileNotFoundError):
                os.unlink(path)


def test_source_server_serves_rust_client(tmp_path: Path):
    clients: list[subprocess.CompletedProcess[str]] = []
    source = SimpleSource()

    asyncio.run(_exercise_server(tmp_path, source, clients))

    [client] = clients
    assert client.returncode == 0, client.stderr
    # 5 messages acked, then 2 read and nacked.
    assert source.unacked == set()
    assert list(source.redeliver) == [sourcer.Offset(b"5"), sourcer.Offset(b"6")]


class _FailingRead(SimpleSource):
    async def read(self, request: sourcer.ReadRequest) -> AsyncIterator[sourcer.Message]:
        raise RuntimeError("read exploded")
        yield


class _ReadReturnsList(SimpleSource):
    async def read(self, request: sourcer.ReadRequest):  # type: ignore[override]
        return []


class _ReadYieldsWrongType(SimpleSource):
    async def read(self, request: sourcer.ReadRequest):  # type: ignore[override]
        yield "not a message"


class _FailingAck(SimpleSource):
    async def ack(self, offsets: list[sourcer.Offset]) -> None:
        raise RuntimeError("ack exploded")


class _SyncPending(SimpleSource):
    def pending(self) -> int | None:  # type: ignore[override]
        return 0


class _NegativePending(SimpleSource):
    async def pending(self) -> int | None:
        return -1


class _BadPartitions(SimpleSource):
    async def active_partitions(self) -> list[int] | None:
        return "0"  # type: ignore[return-value]


@pytest.mark.parametrize(
    ("source", "error", "match"),
    [
        (_FailingRead(), RuntimeError, "read exploded"),
        (_ReadReturnsList(), TypeError, r"source read must be an async generator"),
        (_ReadYieldsWrongType(), TypeError, "source read must yield Message, got str"),
        (_FailingAck(), RuntimeError, "ack exploded"),
        (_SyncPending(), TypeError, r"source pending must be an async function"),
        (_NegativePending(), ValueError, "source pending must not be negative, got -1"),
        (_BadPartitions(), TypeError, r"source active_partitions must return list\[int\] or None, got str"),
    ],
    ids=lambda value: type(value).__name__.strip("_") if isinstance(value, sourcer.Sourcer) else None,
)
def test_source_server_propagates_handler_failure(tmp_path: Path, source, error, match):
    clients: list[subprocess.CompletedProcess[str]] = []
    with pytest.raises(error, match=match):
        asyncio.run(_exercise_server(tmp_path, source, clients))

    # The client must get a gRPC error, not a silent empty or default answer.
    [client] = clients
    assert client.returncode != 0
