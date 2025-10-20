import asyncio
import signal

from pynumaflow_lite import mapper


async def async_handler(
        keys: list[str], payload: mapper.Datum
) -> mapper.Messages:
    messages = mapper.Messages()

    if payload.value == b"bad world":
        messages.append(mapper.Message.message_to_drop())
    else:
        messages.append(mapper.Message(payload.value, keys))

    return messages


async def start(f: callable):
    sock_file = "/tmp/var/run/numaflow/map.sock"
    server_info_file = "/tmp/var/run/numaflow/mapper-server-info"
    server = mapper.MapAsyncServer(sock_file, server_info_file)

    # Register loop-level signal handlers to request graceful shutdown
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        await server.start(f)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        try:
            server.stop()
        except Exception:
            pass
        return


if __name__ == "__main__":
    asyncio.run(start(async_handler))
