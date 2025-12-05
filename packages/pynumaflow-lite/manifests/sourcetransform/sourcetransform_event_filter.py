import asyncio
import signal
from datetime import datetime, timezone
from pynumaflow_lite import sourcetransformer

# Define epoch timestamps for filtering
january_first_2022 = datetime(2022, 1, 1, tzinfo=timezone.utc)
january_first_2023 = datetime(2023, 1, 1, tzinfo=timezone.utc)


class EventFilter(sourcetransformer.SourceTransformer):
    """
    A source transformer that filters and routes messages based on event time.
    
    - Messages before 2022 are dropped
    - Messages within 2022 are tagged with "within_year_2022"
    - Messages after 2022 are tagged with "after_year_2022"
    """
    
    async def handler(
            self, keys: list[str], datum: sourcetransformer.Datum
    ) -> sourcetransformer.Messages:
        val = datum.value
        event_time = datum.event_time
        messages = sourcetransformer.Messages()

        if event_time < january_first_2022:
            print(f"Got event time: {event_time}, it is before 2022, so dropping")
            messages.append(sourcetransformer.Message.message_to_drop(event_time))
        elif event_time < january_first_2023:
            print(f"Got event time: {event_time}, it is within year 2022, so forwarding to within_year_2022")
            messages.append(
                sourcetransformer.Message(
                    value=val,
                    event_time=january_first_2022,
                    keys=keys,
                    tags=["within_year_2022"]
                )
            )
        else:
            print(f"Got event time: {event_time}, it is after year 2022, so forwarding to after_year_2022")
            messages.append(
                sourcetransformer.Message(
                    value=val,
                    event_time=january_first_2023,
                    keys=keys,
                    tags=["after_year_2022"]
                )
            )

        return messages


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass


async def start(f: callable):
    server = sourcetransformer.SourceTransformAsyncServer()

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
    async_handler = EventFilter()
    asyncio.run(start(async_handler))

