import asyncio
import signal
from typing import Awaitable, Callable

from pynumaflow_lite import mapper


class SimpleCat(mapper.Mapper):
    async def handler(
            self, keys: list[str], payload: mapper.Datum
    ) -> mapper.Messages:

        messages = mapper.Messages()

        if payload.value == b"bad world":
            messages.append(mapper.Message.message_to_drop())
        else:
            messages.append(mapper.Message(payload.value, keys))

        return messages


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass


async def start(f: Callable[[list[str], mapper.Datum], Awaitable[mapper.Messages]]):
    server = mapper.MapAsyncServer()

    # Register loop-level signal handlers so we control shutdown and avoid asyncio.run
    # converting it into KeyboardInterrupt/CancelledError traces.
    loop = asyncio.get_running_loop()
    loop.set_debug(True)
    print("Registering signal handlers", loop)
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        print("Failed to register signal handlers")
        # add_signal_handler may not be available on some platforms/contexts; fallback below.
        pass

    try:
        await server.start(f)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        # Fallback in case the task was cancelled by the runner
        try:
            server.stop()
        except Exception:
            pass
        return


if __name__ == "__main__":
    async_handler = SimpleCat()
    asyncio.run(start(async_handler))
