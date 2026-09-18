"""
Testing utilities for Numaflow Sink UDFs.

Provides ``SinkerTestKit`` for integration testing sink handlers through a
real gRPC server, without needing the full Numaflow platform.

Quick start::

    from pynumaflow.sinker.testing import SinkerTestKit, datum

    with SinkerTestKit(my_handler) as kit:
        result = kit.client().send_request(datum(b"hello"), datum(b"world"))
        assert result.all_success()
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
import json
import os
import tempfile
import threading
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional, Union

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow._metadata import UserMetadata, SystemMetadata, _user_and_system_metadata_from_proto
from pynumaflow.proto.common import metadata_pb2
from pynumaflow.proto.sinker import sink_pb2, sink_pb2_grpc
from pynumaflow.sinker._dtypes import Datum, Message, Response, Responses, Sinker
from pynumaflow.sinker.servicer.async_servicer import AsyncSinkServicer
from pynumaflow.sinker.servicer.sync_servicer import SyncSinkServicer

__all__ = ["SinkerTestKit", "Client", "SinkTestResult", "datum"]

_counter = itertools.count()


# ---------------------------------------------------------------------------
# datum factory
# ---------------------------------------------------------------------------


def datum(
    value: Union[bytes, str, dict],
    *,
    keys: Optional[list[str]] = None,
    id: Optional[str] = None,  # noqa: A002
    event_time: Optional[datetime] = None,
    watermark: Optional[datetime] = None,
    headers: Optional[dict[str, str]] = None,
    user_metadata: Optional[UserMetadata] = None,
    system_metadata: Optional[SystemMetadata] = None,
) -> Datum:
    """Build a :class:`~pynumaflow.sinker.Datum` with sensible defaults.

    *value* can be ``bytes``, ``str`` (auto-encoded to UTF-8), or ``dict``
    (auto-serialised to JSON bytes).  All other fields default to safe values
    so you only specify what matters for a given test.

    Args:
        value: Payload for the datum.
        keys: Message keys. Defaults to ``[]``.
        id: Sink message ID. Auto-generated (``"test-0"``, ``"test-1"``, …)
            when not provided.
        event_time: Event time. Defaults to *now* (UTC).
        watermark: Watermark. Defaults to *now* (UTC).
        headers: Optional message headers.
        user_metadata: Optional user metadata.
        system_metadata: Optional system metadata.

    Returns:
        A fully constructed :class:`Datum`.
    """
    if isinstance(value, str):
        value = value.encode("utf-8")
    elif isinstance(value, dict):
        value = json.dumps(value).encode("utf-8")

    now = datetime.now(timezone.utc)
    return Datum(
        keys=keys or [],
        sink_msg_id=id or f"test-{next(_counter)}",
        value=value,
        event_time=event_time or now,
        watermark=watermark or now,
        headers=headers,
        user_metadata=user_metadata,
        system_metadata=system_metadata,
    )


# ---------------------------------------------------------------------------
# SinkTestResult
# ---------------------------------------------------------------------------


class SinkTestResult(Sequence):
    """Wrapper around :class:`~pynumaflow.sinker.Responses` with helpers for
    assertions and filtering."""

    def __init__(self, responses: Responses):
        self._responses = responses

    # -- Sequence interface --------------------------------------------------

    def __len__(self) -> int:
        return len(self._responses)

    def __getitem__(self, index):
        return self._responses[index]

    def __iter__(self):
        return iter(self._responses)

    def __repr__(self) -> str:
        return f"SinkTestResult({self._responses!r})"

    # -- Bulk predicates -----------------------------------------------------

    def all_success(self) -> bool:
        """Return ``True`` if every response has ``success=True``."""
        return len(self._responses) > 0 and all(r.success for r in self._responses)

    def all_failed(self) -> bool:
        """Return ``True`` if every response is a failure (not success,
        fallback, or on_success)."""
        return len(self._responses) > 0 and all(
            not r.success and not r.fallback and not r.on_success for r in self._responses
        )

    def all_fallback(self) -> bool:
        """Return ``True`` if every response has ``fallback=True``."""
        return len(self._responses) > 0 and all(r.fallback for r in self._responses)

    # -- Filtered views ------------------------------------------------------

    @property
    def successes(self) -> list[Response]:
        """All responses where ``success is True``."""
        return [r for r in self._responses if r.success]

    @property
    def failures(self) -> list[Response]:
        """All responses that are failures (not success, fallback, or on_success)."""
        return [
            r
            for r in self._responses
            if not r.success and not r.fallback and not r.on_success
        ]

    @property
    def fallbacks(self) -> list[Response]:
        """All responses where ``fallback is True``."""
        return [r for r in self._responses if r.fallback]

    @property
    def on_successes(self) -> list[Response]:
        """All responses where ``on_success is True``."""
        return [r for r in self._responses if r.on_success]

    # -- Assertion helpers ---------------------------------------------------

    def assert_all_success(self) -> None:
        """Raise :class:`AssertionError` if any response is not successful."""
        failed = [(i, r) for i, r in enumerate(self._responses) if not r.success]
        if failed:
            detail = ", ".join(f"[{i}] id={r.id} err={r.err}" for i, r in failed)
            raise AssertionError(
                f"{len(failed)} of {len(self)} response(s) not successful: {detail}"
            )

    def assert_counts(
        self,
        *,
        success: Optional[int] = None,
        fallback: Optional[int] = None,
        failed: Optional[int] = None,
        on_success: Optional[int] = None,
    ) -> None:
        """Assert the number of responses in each category."""
        if success is not None:
            actual = len(self.successes)
            assert actual == success, f"Expected {success} success(es), got {actual}"
        if fallback is not None:
            actual = len(self.fallbacks)
            assert actual == fallback, f"Expected {fallback} fallback(s), got {actual}"
        if failed is not None:
            actual = len(self.failures)
            assert actual == failed, f"Expected {failed} failure(s), got {actual}"
        if on_success is not None:
            actual = len(self.on_successes)
            assert actual == on_success, f"Expected {on_success} on_success(es), got {actual}"

    def assert_response(
        self,
        index: int,
        *,
        success: Optional[bool] = None,
        fallback: Optional[bool] = None,
        on_success: Optional[bool] = None,
        err: Optional[str] = None,
    ) -> None:
        """Assert properties of a specific response by *index*.

        When *err* is provided it checks that the string is **contained** in
        the response's error message.
        """
        r = self._responses[index]
        if success is not None:
            assert r.success == success, (
                f"Response {index}: expected success={success}, got {r.success}"
            )
        if fallback is not None:
            assert r.fallback == fallback, (
                f"Response {index}: expected fallback={fallback}, got {r.fallback}"
            )
        if on_success is not None:
            assert r.on_success == on_success, (
                f"Response {index}: expected on_success={on_success}, got {r.on_success}"
            )
        if err is not None:
            assert r.err is not None and err in r.err, (
                f"Response {index}: expected err containing {err!r}, got {r.err!r}"
            )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


def _coerce(item: Union[bytes, str, dict, Datum]) -> Datum:
    """Convert a raw value to a :class:`Datum` if it isn't already."""
    if isinstance(item, Datum):
        return item
    return datum(item)


def _build_proto_metadata(d: Datum) -> metadata_pb2.Metadata:
    """Serialize a Datum's user/system metadata into the proto representation."""
    user_meta = (
        {
            group: metadata_pb2.KeyValueGroup(key_value=kv)
            for group, kv in d.user_metadata._data.items()
        }
        if d.user_metadata
        else {}
    )
    sys_meta = (
        {
            group: metadata_pb2.KeyValueGroup(key_value=kv)
            for group, kv in d.system_metadata._data.items()
        }
        if d.system_metadata
        else {}
    )
    return metadata_pb2.Metadata(user_metadata=user_meta, sys_metadata=sys_meta)


def _parse_result(result: sink_pb2.SinkResponse.Result) -> Response:
    """Map a protobuf ``SinkResponse.Result`` back to an SDK ``Response``."""
    if result.status == sink_pb2.Status.SUCCESS:
        return Response.as_success(result.id)
    elif result.status == sink_pb2.Status.FALLBACK:
        return Response.as_fallback(result.id)
    elif result.status == sink_pb2.Status.ON_SUCCESS:
        on_success_msg = None
        if result.HasField("on_success_msg"):
            proto_msg = result.on_success_msg
            user_meta = None
            if proto_msg.HasField("metadata"):
                user_meta, _ = _user_and_system_metadata_from_proto(proto_msg.metadata)
            on_success_msg = Message(
                value=proto_msg.value,
                keys=list(proto_msg.keys) if proto_msg.keys else None,
                user_metadata=user_meta,
            )
        return Response.as_on_success(result.id, on_success_msg)
    else:
        return Response.as_failure(result.id, result.err_msg)


class Client:
    """gRPC client that speaks the full Numaflow sink protocol
    (handshake → data → EOT) and returns SDK-level results.

    Obtain instances via :meth:`SinkerTestKit.client`.
    """

    def __init__(self, address: str):
        self._channel = grpc.insecure_channel(address)
        self._stub = sink_pb2_grpc.SinkStub(self._channel)

    # -- Public API ----------------------------------------------------------

    def send_request(self, *items: Union[bytes, str, dict, Datum]) -> SinkTestResult:
        """Send a batch of datums through the full gRPC streaming protocol.

        Each positional argument can be a :class:`Datum`, ``bytes``, ``str``,
        or ``dict``.  Non-Datum values are auto-wrapped via :func:`datum`.

        Returns:
            A :class:`SinkTestResult` containing the parsed SDK responses.
        """
        datums = [_coerce(item) for item in items]

        def _request_iter():
            yield sink_pb2.SinkRequest(handshake=sink_pb2.Handshake(sot=True))
            for d in datums:
                event_ts = _timestamp_pb2.Timestamp()
                event_ts.FromDatetime(dt=d.event_time)
                watermark_ts = _timestamp_pb2.Timestamp()
                watermark_ts.FromDatetime(dt=d.watermark)
                yield sink_pb2.SinkRequest(
                    request=sink_pb2.SinkRequest.Request(
                        id=d.id,
                        keys=d.keys,
                        value=d.value,
                        event_time=event_ts,
                        watermark=watermark_ts,
                        headers=d.headers,
                        metadata=_build_proto_metadata(d),
                    )
                )
            yield sink_pb2.SinkRequest(status=sink_pb2.TransmissionStatus(eot=True))

        responses: list[Response] = []
        for resp in self._stub.SinkFn(request_iterator=_request_iter()):
            if resp.HasField("handshake") and resp.handshake.sot:
                continue
            if resp.HasField("status") and resp.status.eot:
                continue
            for result in resp.results:
                responses.append(_parse_result(result))

        return SinkTestResult(Responses(*responses))

    def is_ready(self) -> bool:
        """Call the ``IsReady`` RPC and return the readiness flag."""
        resp = self._stub.IsReady(request=_empty_pb2.Empty())
        return resp.ready

    # -- Lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Shut down the underlying gRPC channel."""
        self._channel.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# ---------------------------------------------------------------------------
# SinkerTestKit
# ---------------------------------------------------------------------------


class SinkerTestKit:
    """Integration test kit that starts a **real** gRPC server for a sink
    handler.

    Works with sync and async handlers, both function-based and
    :class:`~pynumaflow.sinker.Sinker` subclass-based.

    Args:
        sinker_handler: The sink handler to test (function or Sinker instance).
        max_threads: Maximum number of server threads (default 4).

    Example with *pytest*::

        @pytest.fixture(scope="module")
        def sink_kit():
            with SinkerTestKit(my_handler) as kit:
                yield kit

        def test_batch(sink_kit):
            result = sink_kit.client().send_request(datum(b"msg1"), datum(b"msg2"))
            result.assert_all_success()

    Example with *unittest*::

        class TestMySink(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls.kit = SinkerTestKit(my_handler)
                cls.kit.start_server()

            @classmethod
            def tearDownClass(cls):
                cls.kit.stop_server()

            def test_batch(self):
                result = self.kit.client().send_request(datum(b"msg"))
                self.assertTrue(result.all_success())
    """

    def __init__(self, sinker_handler, max_threads: int = 4):
        self._handler = sinker_handler
        self._max_threads = max_threads
        self._is_async = self._detect_async(sinker_handler)

        self._tmp_dir = tempfile.mkdtemp(prefix="numaflow-test-")
        self._sock_path = os.path.join(self._tmp_dir, "sink.sock")
        self._address = f"unix://{self._sock_path}"

        self._server = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    # -- Public API ----------------------------------------------------------

    def start_server(self) -> None:
        """Start the gRPC server in the background. Blocks until the server
        is ready to accept connections."""
        if self._is_async:
            self._start_async()
        else:
            self._start_sync()
        self._wait_for_ready()

    def stop_server(self) -> None:
        """Gracefully stop the gRPC server and clean up resources."""
        if self._server is not None:
            if self._is_async and self._loop is not None:
                future = asyncio.run_coroutine_threadsafe(
                    self._server.stop(grace=2), self._loop
                )
                try:
                    future.result(timeout=5)
                except Exception:
                    pass
                self._loop.call_soon_threadsafe(self._loop.stop)
                if self._thread is not None:
                    self._thread.join(timeout=5)
            else:
                self._server.stop(grace=2)
            self._server = None

        try:
            if os.path.exists(self._sock_path):
                os.remove(self._sock_path)
            if os.path.exists(self._tmp_dir):
                os.rmdir(self._tmp_dir)
        except OSError:
            pass

    def client(self) -> Client:
        """Create a new :class:`Client` connected to this server."""
        return Client(self._address)

    # -- Context manager -----------------------------------------------------

    def __enter__(self) -> SinkerTestKit:
        self.start_server()
        return self

    def __exit__(self, *exc) -> None:
        self.stop_server()

    # -- Internals -----------------------------------------------------------

    @staticmethod
    def _detect_async(handler) -> bool:
        if isinstance(handler, Sinker):
            return inspect.iscoroutinefunction(handler.handler)
        return inspect.iscoroutinefunction(handler)

    def _start_sync(self) -> None:
        servicer = SyncSinkServicer(self._handler)
        server = grpc.server(ThreadPoolExecutor(max_workers=self._max_threads))
        sink_pb2_grpc.add_SinkServicer_to_server(servicer, server)
        server.add_insecure_port(self._address)
        server.start()
        self._server = server

    @staticmethod
    def _grpc_aio_exception_handler(loop, context):
        """Suppress BlockingIOError from grpc-aio's completion queue poller
        during shutdown.  All other exceptions use the default handler."""
        exception = context.get("exception")
        if isinstance(exception, BlockingIOError):
            return
        loop.default_exception_handler(context)

    def _start_async(self) -> None:
        loop = asyncio.new_event_loop()
        loop.set_exception_handler(self._grpc_aio_exception_handler)
        self._loop = loop
        ready_event = threading.Event()

        def _run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        self._thread = threading.Thread(target=_run_loop, daemon=True)
        self._thread.start()

        async def _start():
            servicer = AsyncSinkServicer(self._handler)
            server = grpc.aio.server()
            sink_pb2_grpc.add_SinkServicer_to_server(servicer, server)
            server.add_insecure_port(self._address)
            await server.start()
            self._server = server
            ready_event.set()
            await server.wait_for_termination()

        asyncio.run_coroutine_threadsafe(_start(), loop)
        ready_event.wait(timeout=10)

    def _wait_for_ready(self, timeout: int = 10) -> None:
        channel = grpc.insecure_channel(self._address)
        try:
            grpc.channel_ready_future(channel).result(timeout=timeout)
        finally:
            channel.close()
