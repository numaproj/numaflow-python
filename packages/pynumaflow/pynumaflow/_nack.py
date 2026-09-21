from __future__ import annotations

from dataclasses import dataclass, field

from pynumaflow.proto.common import nack_options_pb2


@dataclass
class NackOptions:
    """Per-message redelivery options for a nack.

    Args:
        delay: the redelivery delay in milliseconds.
        max_deliveries: the maximum number of redelivery attempts.
        reason: a human-readable reason for nacking the message.
        nack_map: a generic map of string key-value pairs used to pass
            properties/configurations for nacking back to the source (e.g. for
            sources like SQS, Pulsar or JetStream that support NACK).
    """

    delay: int | None = None
    max_deliveries: int | None = None
    reason: str | None = None
    nack_map: dict[str, str] = field(default_factory=dict)

    def _to_proto(self) -> nack_options_pb2.NackOptions:
        return nack_options_pb2.NackOptions(
            reason=self.reason,
            max_deliveries=self.max_deliveries,
            delay=self.delay,
            nack_map=self.nack_map,
        )


def _nack_options_to_proto(
    opts: NackOptions | None,
) -> nack_options_pb2.NackOptions | None:
    if opts is None:
        return None
    return opts._to_proto()


def _nack_options_from_proto(
    proto: nack_options_pb2.NackOptions,
) -> NackOptions:
    return NackOptions(
        delay=proto.delay if proto.HasField("delay") else None,
        max_deliveries=proto.max_deliveries if proto.HasField("max_deliveries") else None,
        reason=proto.reason if proto.HasField("reason") else None,
        # proto3 map fields do not support HasField; an unset map is simply empty.
        nack_map=dict(proto.nack_map),
    )
