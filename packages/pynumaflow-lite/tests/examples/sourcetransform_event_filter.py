import asyncio
from datetime import datetime, timezone

from pynumaflow_lite.sourcetransformer import Datum, Message, SourceTransformAsyncServer, SourceTransformer

january_first_2022 = datetime(2022, 1, 1, tzinfo=timezone.utc)
january_first_2023 = datetime(2023, 1, 1, tzinfo=timezone.utc)


class EventFilter(SourceTransformer):
    """
    A source transformer that filters and routes messages based on event time.

    - Messages before 2022 are dropped
    - Messages within 2022 are tagged with "within_year_2022"
    - Messages after 2022 are tagged with "after_year_2022"

    Also demonstrates reading and creating metadata.
    """

    async def handler(self, datum: Datum) -> list[Message]:
        # Read system metadata (read-only)
        print(f"System metadata groups: {list(datum.system_metadata)}")
        for group, key_values in datum.system_metadata.items():
            for key, value in key_values.items():
                print(f"  System[{group}][{key}] = {value}")

        # Read user metadata
        print(f"User metadata groups: {list(datum.user_metadata)}")
        for group, key_values in datum.user_metadata.items():
            for key, value in key_values.items():
                print(f"  User[{group}][{key}] = {value}")

        event_time = datum.event_time
        if event_time < january_first_2022:
            print(f"Got event time: {event_time}, it is before 2022, so dropping")
            return [Message.to_drop(event_time)]

        if event_time < january_first_2023:
            print(f"Got event time: {event_time}, it is within year 2022, so forwarding to within_year_2022")
            tag, new_event_time = "within_year_2022", january_first_2022
        else:
            print(f"Got event time: {event_time}, it is after year 2022, so forwarding to after_year_2022")
            tag, new_event_time = "after_year_2022", january_first_2023

        # Forward the incoming user metadata, echo the system metadata, and add the filter result
        user_metadata = {
            **datum.user_metadata,
            "system_echo": {
                f"{group}.{key}": value
                for group, key_values in datum.system_metadata.items()
                for key, value in key_values.items()
            },
            "filter_info": {
                "filter_result": tag.encode(),
                "original_event_time": str(event_time).encode(),
            },
        }
        return [
            Message(
                datum.value,
                new_event_time,
                keys=datum.keys,
                tags=[tag],
                user_metadata=user_metadata,
            )
        ]


async def main() -> None:
    await SourceTransformAsyncServer(
        EventFilter(),
        sock_file="/tmp/var/run/numaflow/sourcetransform.sock",
        server_info_file="/tmp/var/run/numaflow/sourcetransformer-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
