from __future__ import annotations

from dataclasses import dataclass

from pynumaflow.proto.common import nack_options_pb2


@dataclass
class NackOptions:
    """Per-message redelivery options for a nack."""

    delay: int | None = None
    max_deliveries: int | None = None
    reason: str | None = None

    def _to_proto(self) -> nack_options_pb2.NackOptions:
        return nack_options_pb2.NackOptions(
            reason=self.reason,
            max_deliveries=self.max_deliveries,
            delay=self.delay,
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
    )
