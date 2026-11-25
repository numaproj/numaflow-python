"""Tests for the log sink example handlers."""

import pytest

from example import UserDefinedSink, udsink_handler
from pynumaflow.sinker.testing import SinkerTestKit, datum


@pytest.fixture(scope="module")
def sinker_kit():
    with SinkerTestKit(udsink_handler) as kit:
        yield kit


class TestFuncHandler:
    def test_is_ready(self, sinker_kit):
        with sinker_kit.client() as c:
            assert c.is_ready()

    def test_single_datum(self, sinker_kit):
        with sinker_kit.client() as c:
            result = c.send_request(datum(b"hello"))
        assert len(result) == 1
        result.assert_all_success()

    def test_multiple_datums(self, sinker_kit):
        with sinker_kit.client() as c:
            result = c.send_request(
                datum(b"msg-1"),
                datum(b"msg-2"),
                datum(b"msg-3"),
            )
        assert len(result) == 3
        result.assert_all_success()
        result.assert_counts(success=3, failed=0, fallback=0)

    def test_str_payload(self, sinker_kit):
        with sinker_kit.client() as c:
            result = c.send_request(datum("string payload"))
        result.assert_all_success()

    def test_dict_payload(self, sinker_kit):
        with sinker_kit.client() as c:
            result = c.send_request(datum({"event": "click", "count": 1}))
        result.assert_all_success()

    def test_with_keys(self, sinker_kit):
        with sinker_kit.client() as c:
            result = c.send_request(datum(b"keyed", keys=["k1", "k2"]))
        result.assert_all_success()

    def test_with_headers(self, sinker_kit):
        with sinker_kit.client() as c:
            result = c.send_request(
                datum(b"with-headers", headers={"x-trace-id": "abc123"})
            )
        result.assert_all_success()

    def test_auto_coerce(self, sinker_kit):
        """send_request accepts raw bytes/str/dict without wrapping in datum()."""
        with sinker_kit.client() as c:
            result = c.send_request(b"raw", "text", {"k": "v"})
        assert len(result) == 3
        result.assert_all_success()

    def test_large_batch(self, sinker_kit):
        with sinker_kit.client() as c:
            result = c.send_request(*[datum(f"item-{i}") for i in range(50)])
        assert len(result) == 50
        result.assert_all_success()


# ---------------------------------------------------------------------------
# Class-based handler (UserDefinedSink)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def class_kit():
    with SinkerTestKit(UserDefinedSink()) as kit:
        yield kit


class TestClassHandler:
    def test_is_ready(self, class_kit):
        with class_kit.client() as c:
            assert c.is_ready()

    def test_single_datum(self, class_kit):
        with class_kit.client() as c:
            result = c.send_request(datum(b"hello"))
        assert len(result) == 1
        result.assert_all_success()

    def test_multiple_datums(self, class_kit):
        with class_kit.client() as c:
            result = c.send_request(
                datum(b"msg-1"),
                datum(b"msg-2"),
                datum(b"msg-3"),
            )
        assert len(result) == 3
        result.assert_all_success()
        result.assert_counts(success=3, failed=0, fallback=0)

    def test_str_payload(self, class_kit):
        with class_kit.client() as c:
            result = c.send_request(datum("string payload"))
        result.assert_all_success()

    def test_dict_payload(self, class_kit):
        with class_kit.client() as c:
            result = c.send_request(datum({"event": "click", "count": 1}))
        result.assert_all_success()

    def test_with_keys(self, class_kit):
        with class_kit.client() as c:
            result = c.send_request(datum(b"keyed", keys=["k1", "k2"]))
        result.assert_all_success()

    def test_auto_coerce(self, class_kit):
        with class_kit.client() as c:
            result = c.send_request(b"raw", "text", {"k": "v"})
        assert len(result) == 3
        result.assert_all_success()

    def test_large_batch(self, class_kit):
        with class_kit.client() as c:
            result = c.send_request(*[datum(f"item-{i}") for i in range(50)])
        assert len(result) == 50
        result.assert_all_success()
