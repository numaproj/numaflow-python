import asyncio
from datetime import datetime, timezone

from pynumaflow_lite.sourcetransformer import Datum, Message, SourceTransformAsyncServer

january_first_2022 = datetime(2022, 1, 1, tzinfo=timezone.utc)
january_first_2023 = datetime(2023, 1, 1, tzinfo=timezone.utc)


async def event_filter(datum: Datum) -> list[Message]:
    event_time = datum.event_time
    if event_time < january_first_2022:
        return [Message.to_drop(event_time)]

    if event_time < january_first_2023:
        tag, new_event_time = "within_year_2022", january_first_2022
    else:
        tag, new_event_time = "after_year_2022", january_first_2023

    user_metadata = {
        "filter_info": {
            "filter_result": tag.encode(),
            "original_event_time": str(event_time).encode(),
        }
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
        event_filter,
        sock_file="/tmp/var/run/numaflow/sourcetransform.sock",
        server_info_file="/tmp/var/run/numaflow/sourcetransformer-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
