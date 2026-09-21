from pynumaflow._nack import (
    NackOptions,
    _nack_options_to_proto,
    _nack_options_from_proto,
)
from pynumaflow.proto.common import nack_options_pb2


class TestNackOptions:
    """Tests for the NackOptions dataclass."""

    def test_defaults_all_none(self):
        opts = NackOptions()
        assert opts.delay is None
        assert opts.max_deliveries is None
        assert opts.reason is None
        assert opts.nack_map == {}

    def test_with_values(self):
        opts = NackOptions(
            delay=5000,
            max_deliveries=3,
            reason="downstream down",
            nack_map={"visibility_timeout": "30"},
        )
        assert opts.delay == 5000
        assert opts.max_deliveries == 3
        assert opts.reason == "downstream down"
        assert opts.nack_map == {"visibility_timeout": "30"}

    def test_default_nack_map_not_shared(self):
        # default_factory must give each instance its own dict.
        a = NackOptions()
        b = NackOptions()
        a.nack_map["k"] = "v"
        assert b.nack_map == {}

    def test_equality(self):
        assert NackOptions(delay=1, max_deliveries=2, reason="x") == NackOptions(
            delay=1, max_deliveries=2, reason="x"
        )
        assert NackOptions(delay=1) != NackOptions(delay=2)
        assert NackOptions(nack_map={"a": "1"}) != NackOptions(nack_map={"a": "2"})


class TestToProto:
    """Tests for NackOptions._to_proto and _nack_options_to_proto."""

    def test_to_proto_maps_all_fields(self):
        proto = NackOptions(
            delay=5000,
            max_deliveries=3,
            reason="retry",
            nack_map={"visibility_timeout": "30"},
        )._to_proto()
        assert isinstance(proto, nack_options_pb2.NackOptions)
        assert proto.delay == 5000
        assert proto.max_deliveries == 3
        assert proto.reason == "retry"
        assert dict(proto.nack_map) == {"visibility_timeout": "30"}

    def test_to_proto_empty_nack_map(self):
        proto = NackOptions()._to_proto()
        assert dict(proto.nack_map) == {}

    def test_nack_options_to_proto_none_returns_none(self):
        assert _nack_options_to_proto(None) is None

    def test_nack_options_to_proto_with_opts(self):
        proto = _nack_options_to_proto(NackOptions(delay=100, max_deliveries=1, reason="r"))
        assert isinstance(proto, nack_options_pb2.NackOptions)
        assert proto.delay == 100
        assert proto.max_deliveries == 1
        assert proto.reason == "r"


class TestFromProto:
    """Tests for _nack_options_from_proto (HasField handling)."""

    def test_unset_fields_become_none(self):
        # An empty proto has no fields set; every field should come back as None.
        opts = _nack_options_from_proto(nack_options_pb2.NackOptions())
        assert opts.delay is None
        assert opts.max_deliveries is None
        assert opts.reason is None
        # An unset map field comes back as an empty dict (no HasField for maps).
        assert opts.nack_map == {}

    def test_set_fields_are_returned(self):
        proto = nack_options_pb2.NackOptions(
            delay=5000,
            max_deliveries=3,
            reason="retry",
            nack_map={"visibility_timeout": "30"},
        )
        opts = _nack_options_from_proto(proto)
        assert opts.delay == 5000
        assert opts.max_deliveries == 3
        assert opts.reason == "retry"
        assert opts.nack_map == {"visibility_timeout": "30"}

    def test_explicit_zero_is_preserved(self):
        # Because the proto fields are `optional`, an explicitly-set zero must be
        # distinguished from "unset" and preserved (not collapsed to None).
        proto = nack_options_pb2.NackOptions(delay=0, max_deliveries=0, reason="")
        opts = _nack_options_from_proto(proto)
        assert opts.delay == 0
        assert opts.max_deliveries == 0
        assert opts.reason == ""

    def test_partial_fields(self):
        proto = nack_options_pb2.NackOptions(max_deliveries=2)
        opts = _nack_options_from_proto(proto)
        assert opts.max_deliveries == 2
        assert opts.delay is None
        assert opts.reason is None


class TestRoundTrip:
    """Round-trip conversions between NackOptions and its proto form."""

    def test_round_trip_full(self):
        original = NackOptions(
            delay=5000,
            max_deliveries=3,
            reason="retry",
            nack_map={"visibility_timeout": "30", "queue": "dlq"},
        )
        restored = _nack_options_from_proto(_nack_options_to_proto(original))
        assert restored == original

    def test_round_trip_defaults(self):
        original = NackOptions()
        restored = _nack_options_from_proto(_nack_options_to_proto(original))
        assert restored == original
