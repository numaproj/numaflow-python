import logging
import os
from collections.abc import AsyncIterable

from pynumaflow import setup_logging
from pynumaflow.accumulator import Accumulator, AccumulatorAsyncServer
from pynumaflow.accumulator import (
    Message,
    Datum,
)
from pynumaflow.shared.asynciter import NonBlockingIterator

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)


class Blackhole(Accumulator):
    """Blackhole is an accumulator that intentionally discards every datum it receives
    without forwarding any data downstream.

    A naive implementation would simply read the input stream and emit nothing. However, an
    accumulator that never emits anything for the datums it consumes leaves the framework unable
    to release the per-datum tracked state, leading to unbounded memory growth.

    Instead, this example emits a drop message for every datum using ``Message.to_drop(datum)``.
    A drop message is not forwarded to the next vertex, but it still allows the framework to
    advance the watermark and release the tracked state (WAL) for that datum - giving us
    "blackhole" semantics without leaking memory. This pattern is useful for multiplexer-,
    cross-join-, or filter-style accumulators that legitimately need to omit some (or all) of
    their inputs.
    """

    def __init__(self):
        _LOGGER.info("Blackhole initialized")

    async def handler(
        self,
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
    ):
        _LOGGER.info("Blackhole handler started")
        async for datum in datums:
            _LOGGER.info(
                f"Dropping datum with event time: {datum.event_time}, "
                f"watermark: {datum.watermark}"
            )
            # Emit a drop message: nothing is forwarded downstream, but the framework still
            # advances the watermark and releases the tracked state for this datum.
            await output.put(Message.to_drop(datum))
        _LOGGER.info("Timeout reached")


if __name__ == "__main__":
    grpc_server = AccumulatorAsyncServer(Blackhole)
    grpc_server.start()
