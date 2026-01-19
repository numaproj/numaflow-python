import asyncio
import signal
from pynumaflow_lite import mapper


class SimpleCat(mapper.Mapper):
    async def handler(
            self, keys: list[str], payload: mapper.Datum
    ) -> mapper.Messages:

        messages = mapper.Messages()

        # Read system metadata (read-only)
        print(f"System metadata groups: {payload.system_metadata.groups()}")
        for group in payload.system_metadata.groups():
            for key in payload.system_metadata.keys(group):
                value = payload.system_metadata.value(group, key)
                print(f"  System[{group}][{key}] = {value}")

        # Read user metadata (read-only from input)
        print(f"User metadata groups: {payload.user_metadata.groups()}")
        for group in payload.user_metadata.groups():
            for key in payload.user_metadata.keys(group):
                value = payload.user_metadata.value(group, key)
                print(f"  User[{group}][{key}] = {value}")

        if payload.value == b"bad world":
            messages.append(mapper.Message.message_to_drop())
        else:
            # Create user metadata for the outgoing message
            user_metadata = mapper.UserMetadata()
            user_metadata.create_group("processing")
            user_metadata.add_kv("processing", "handler", b"map_cat_class")
            user_metadata.add_kv("processing", "msg_length", str(len(payload.value)).encode())

            messages.append(mapper.Message(payload.value, keys, user_metadata=user_metadata))

        return messages


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass


async def start(f: callable):
    sock_file = "/tmp/var/run/numaflow/map.sock"
    server_info_file = "/tmp/var/run/numaflow/mapper-server-info"
    server = mapper.MapAsyncServer(sock_file, server_info_file)

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
