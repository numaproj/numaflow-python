"""Tests for pynumaflow.sinker.testing (SinkerTestKit)."""

import json
from collections.abc import AsyncIterable, Iterator
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pynumaflow._metadata import UserMetadata
from pynumaflow.sinker import Datum, Response, Responses, Sinker, Message
from pynumaflow.sinker.testing import SinkerTestKit, SinkTestResult, datum
from tests.testing_utils import mock_terminate_on_stop


# ---------------------------------------------------------------------------
# Sample handlers used by the tests
# ---------------------------------------------------------------------------


def sync_success_handler(datums: Iterator[Datum]) -> Responses:
    responses = Responses()
    for msg in datums:
        responses.append(Response.as_success(msg.id))
    return responses


def sync_routing_handler(datums: Iterator[Datum]) -> Responses:
    """Routes based on payload content."""
    responses = Responses()
    for msg in datums:
        text = msg.value.decode("utf-8")
        if text == "fail":
            responses.append(Response.as_failure(msg.id, "bad data"))
        elif text == "fallback":
            responses.append(Response.as_fallback(msg.id))
        elif text == "on_success":
            responses.append(
                Response.as_on_success(msg.id, Message(msg.value, msg.keys, msg.user_metadata))
            )
        elif text == "on_success_none":
            responses.append(Response.as_on_success(msg.id, None))
        else:
            responses.append(Response.as_success(msg.id))
    return responses


class SyncSinkClass(Sinker):
    def handler(self, datums: Iterator[Datum]) -> Responses:
        responses = Responses()
        for msg in datums:
            responses.append(Response.as_success(msg.id))
        return responses


async def async_success_handler(datums: AsyncIterable[Datum]) -> Responses:
    responses = Responses()
    async for msg in datums:
        responses.append(Response.as_success(msg.id))
    return responses


async def async_routing_handler(datums: AsyncIterable[Datum]) -> Responses:
    responses = Responses()
    async for msg in datums:
        text = msg.value.decode("utf-8")
        if text == "fail":
            responses.append(Response.as_failure(msg.id, "bad data"))
        elif text == "fallback":
            responses.append(Response.as_fallback(msg.id))
        else:
            responses.append(Response.as_success(msg.id))
    return responses


class AsyncSinkClass(Sinker):
    async def handler(self, datums: AsyncIterable[Datum]) -> Responses:
        responses = Responses()
        async for msg in datums:
            responses.append(Response.as_success(msg.id))
        return responses


# ---------------------------------------------------------------------------
# datum() factory tests
# ---------------------------------------------------------------------------


class TestDatumFactory:
    def test_bytes_value(self):
        d = datum(b"hello")
        assert d.value == b"hello"
        assert d.keys == []
        assert d.id.startswith("test-")
        assert d.event_time is not None
        assert d.watermark is not None

    def test_str_value(self):
        d = datum("hello world")
        assert d.value == b"hello world"

    def test_dict_value(self):
        d = datum({"key": "value", "num": 42})
        parsed = json.loads(d.value.decode("utf-8"))
        assert parsed["key"] == "value"
        assert parsed["num"] == 42

    def test_custom_fields(self):
        et = datetime(2025, 1, 1, tzinfo=timezone.utc)
        wm = datetime(2025, 1, 1, tzinfo=timezone.utc)
        d = datum(
            b"payload",
            keys=["k1", "k2"],
            id="custom-id",
            event_time=et,
            watermark=wm,
            headers={"h1": "v1"},
        )
        assert d.keys == ["k1", "k2"]
        assert d.id == "custom-id"
        assert d.event_time == et
        assert d.watermark == wm
        assert d.headers == {"h1": "v1"}

    def test_auto_incremented_ids(self):
        d1 = datum(b"a")
        d2 = datum(b"b")
        assert d1.id != d2.id

    def test_user_metadata(self):
        meta = UserMetadata({"grp": {"k": b"v"}})
        d = datum(b"x", user_metadata=meta)
        assert d.user_metadata.value("grp", "k") == b"v"


# ---------------------------------------------------------------------------
# SinkTestResult tests
# ---------------------------------------------------------------------------


class TestSinkTestResult:
    def test_all_success(self):
        r = SinkTestResult(Responses(Response.as_success("1"), Response.as_success("2")))
        assert r.all_success()
        assert len(r.successes) == 2
        assert len(r.failures) == 0

    def test_all_failed(self):
        r = SinkTestResult(
            Responses(Response.as_failure("1", "err1"), Response.as_failure("2", "err2"))
        )
        assert r.all_failed()
        assert not r.all_success()

    def test_mixed(self):
        r = SinkTestResult(
            Responses(
                Response.as_success("1"),
                Response.as_failure("2", "err"),
                Response.as_fallback("3"),
                Response.as_on_success("4"),
            )
        )
        assert not r.all_success()
        assert len(r.successes) == 1
        assert len(r.failures) == 1
        assert len(r.fallbacks) == 1
        assert len(r.on_successes) == 1

    def test_assert_all_success_raises(self):
        r = SinkTestResult(
            Responses(Response.as_success("1"), Response.as_failure("2", "oops"))
        )
        with pytest.raises(AssertionError, match="1 of 2"):
            r.assert_all_success()

    def test_assert_counts(self):
        r = SinkTestResult(
            Responses(
                Response.as_success("1"),
                Response.as_success("2"),
                Response.as_fallback("3"),
            )
        )
        r.assert_counts(success=2, fallback=1)
        with pytest.raises(AssertionError):
            r.assert_counts(success=3)

    def test_assert_response(self):
        r = SinkTestResult(
            Responses(Response.as_success("1"), Response.as_failure("2", "bad data"))
        )
        r.assert_response(0, success=True)
        r.assert_response(1, success=False, err="bad")

    def test_sequence_protocol(self):
        r = SinkTestResult(Responses(Response.as_success("1"), Response.as_success("2")))
        assert len(r) == 2
        assert r[0].id == "1"
        assert r[1].id == "2"
        assert [resp.id for resp in r] == ["1", "2"]

    def test_empty(self):
        r = SinkTestResult(Responses())
        assert len(r) == 0
        assert not r.all_success()
        assert not r.all_failed()

    def test_all_fallback(self):
        r = SinkTestResult(Responses(Response.as_fallback("1"), Response.as_fallback("2")))
        assert r.all_fallback()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sync_kit():
    with patch("psutil.Process.kill", mock_terminate_on_stop):
        with SinkerTestKit(sync_success_handler) as kit:
            yield kit


@pytest.fixture(scope="module")
def sync_routing_kit():
    with patch("psutil.Process.kill", mock_terminate_on_stop):
        with SinkerTestKit(sync_routing_handler) as kit:
            yield kit


@pytest.fixture(scope="module")
def sync_class_kit():
    with patch("psutil.Process.kill", mock_terminate_on_stop):
        with SinkerTestKit(SyncSinkClass()) as kit:
            yield kit


@pytest.fixture(scope="module")
def async_kit():
    with patch("psutil.Process.kill", mock_terminate_on_stop):
        with SinkerTestKit(async_success_handler) as kit:
            yield kit


@pytest.fixture(scope="module")
def async_routing_kit():
    with patch("psutil.Process.kill", mock_terminate_on_stop):
        with SinkerTestKit(async_routing_handler) as kit:
            yield kit


@pytest.fixture(scope="module")
def async_class_kit():
    with patch("psutil.Process.kill", mock_terminate_on_stop):
        with SinkerTestKit(AsyncSinkClass()) as kit:
            yield kit


# ---------------------------------------------------------------------------
# SinkerTestKit integration tests — sync handler
# ---------------------------------------------------------------------------


class TestSinkerTestKitSync:
    def test_is_ready(self, sync_kit):
        with sync_kit.client() as c:
            assert c.is_ready()

    def test_send_bytes(self, sync_kit):
        with sync_kit.client() as c:
            result = c.send_request(datum(b"hello"), datum(b"world"))
        assert len(result) == 2
        result.assert_all_success()

    def test_send_many(self, sync_kit):
        with sync_kit.client() as c:
            result = c.send_request(*[datum(f"msg-{i}") for i in range(20)])
        assert len(result) == 20
        result.assert_all_success()


class TestSinkerTestKitSyncRouting:
    def test_success(self, sync_routing_kit):
        with sync_routing_kit.client() as c:
            result = c.send_request(datum(b"ok"))
        result.assert_all_success()

    def test_failure(self, sync_routing_kit):
        with sync_routing_kit.client() as c:
            result = c.send_request(datum(b"fail"))
        assert result.all_failed()
        result.assert_response(0, err="bad data")

    def test_fallback(self, sync_routing_kit):
        with sync_routing_kit.client() as c:
            result = c.send_request(datum(b"fallback"))
        assert result.all_fallback()

    def test_on_success_with_message(self, sync_routing_kit):
        meta = UserMetadata({"grp": {"k": b"v"}})
        with sync_routing_kit.client() as c:
            result = c.send_request(datum(b"on_success", keys=["k1"], user_metadata=meta))
        assert len(result.on_successes) == 1
        resp = result[0]
        assert resp.on_success
        assert resp.on_success_msg is not None
        assert resp.on_success_msg.value == b"on_success"
        assert resp.on_success_msg.keys == ["k1"]

    def test_on_success_none_message(self, sync_routing_kit):
        with sync_routing_kit.client() as c:
            result = c.send_request(datum(b"on_success_none"))
        assert len(result.on_successes) == 1
        assert result[0].on_success_msg is None

    def test_mixed_batch(self, sync_routing_kit):
        with sync_routing_kit.client() as c:
            result = c.send_request(
                datum(b"ok"),
                datum(b"fail"),
                datum(b"fallback"),
                datum(b"on_success"),
                datum(b"ok"),
            )
        result.assert_counts(success=2, failed=1, fallback=1, on_success=1)


class TestSinkerTestKitSyncClass:
    def test_success(self, sync_class_kit):
        with sync_class_kit.client() as c:
            result = c.send_request(datum(b"msg1"), datum(b"msg2"))
        result.assert_all_success()


# ---------------------------------------------------------------------------
# SinkerTestKit integration tests — async handler
# ---------------------------------------------------------------------------


class TestSinkerTestKitAsync:
    def test_is_ready(self, async_kit):
        with async_kit.client() as c:
            assert c.is_ready()

    def test_send_bytes(self, async_kit):
        with async_kit.client() as c:
            result = c.send_request(datum(b"hello"), datum(b"world"))
        assert len(result) == 2
        result.assert_all_success()


class TestSinkerTestKitAsyncRouting:
    def test_mixed(self, async_routing_kit):
        with async_routing_kit.client() as c:
            result = c.send_request(
                datum(b"ok"),
                datum(b"fail"),
                datum(b"fallback"),
            )
        result.assert_counts(success=1, failed=1, fallback=1)


class TestSinkerTestKitAsyncClass:
    def test_success(self, async_class_kit):
        with async_class_kit.client() as c:
            result = c.send_request(datum(b"msg1"), datum(b"msg2"))
        result.assert_all_success()


# ---------------------------------------------------------------------------
# Context manager tests
# ---------------------------------------------------------------------------


class TestSinkerTestKitContextManager:
    def test_sync_context_manager(self):
        with patch("psutil.Process.kill", mock_terminate_on_stop):
            with SinkerTestKit(sync_success_handler) as kit:
                result = kit.client().send_request(datum(b"hello"))
                result.assert_all_success()

    def test_async_context_manager(self):
        with patch("psutil.Process.kill", mock_terminate_on_stop):
            with SinkerTestKit(async_success_handler) as kit:
                result = kit.client().send_request(datum(b"hello"))
                result.assert_all_success()


# ---------------------------------------------------------------------------
# Auto-coerce tests (bytes/str/dict passed directly to send_request)
# ---------------------------------------------------------------------------


class TestSinkerTestKitAutoCoerce:
    def test_coerce_bytes(self, sync_kit):
        result = sync_kit.client().send_request(b"raw bytes")
        result.assert_all_success()

    def test_coerce_str(self, sync_kit):
        result = sync_kit.client().send_request("a string")
        result.assert_all_success()

    def test_coerce_dict(self, sync_kit):
        result = sync_kit.client().send_request({"event": "click"})
        result.assert_all_success()

    def test_coerce_mixed(self, sync_kit):
        result = sync_kit.client().send_request(
            b"bytes",
            "string",
            {"dict": True},
            datum(b"explicit"),
        )
        assert len(result) == 4
        result.assert_all_success()
