import asyncio
from datetime import UTC, datetime

from pynumaflow_lite.sourcetransformer import Datum, Message, SourceTransformAsyncServer, SourceTransformer

january_first_2022 = datetime(2022, 1, 1, tzinfo=UTC)
january_first_2023 = datetime(2023, 1, 1, tzinfo=UTC)


class EventFilter(SourceTransformer):
    """
    A source transformer that filters and routes messages based on event time.

    - Messages before 2022 are dropped
    - Messages within 2022 are tagged with "within_year_2022"
    - Messages after 2022 are tagged with "after_year_2022"
    """

    async def handler(self, datum: Datum) -> list[Message]:
        event_time = datum.event_time
        if event_time < january_first_2022:
            print(f"Got event time: {event_time}, it is before 2022, so dropping")
            return [Message.to_drop(event_time)]

        if event_time < january_first_2023:
            print(f"Got event time: {event_time}, it is within year 2022, so forwarding to within_year_2022")
            return [Message(datum.value, january_first_2022, keys=datum.keys, tags=["within_year_2022"])]

        print(f"Got event time: {event_time}, it is after year 2022, so forwarding to after_year_2022")
        return [Message(datum.value, january_first_2023, keys=datum.keys, tags=["after_year_2022"])]


async def main() -> None:
    await SourceTransformAsyncServer(EventFilter()).serve()


if __name__ == "__main__":
    asyncio.run(main())
