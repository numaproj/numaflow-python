from datetime import datetime, timezone

from pynumaflow.sourcetransformer import (
    Datum,
    Message,
    Messages,
    SourceTransformer,
    SourceTransformAsyncServer,
)

SOCK_PATH = "/tmp/var/run/numaflow/sourcetransform.sock"
SERVER_INFO = "/tmp/var/run/numaflow/sourcetransformer-server-info"

# Boundaries are tz-aware UTC so they compare correctly against the tz-aware
# event times handed in by the (Rust) engine.
january_first_2022 = datetime(2022, 1, 1, tzinfo=timezone.utc)
january_first_2023 = datetime(2023, 1, 1, tzinfo=timezone.utc)


class EventFilter(SourceTransformer):
    """
    A source transformer that filters and routes messages based on event time.

    - Messages before 2022 are dropped.
    - Messages within 2022 are tagged "within_year_2022" and re-stamped to Jan 1 2022.
    - Messages after 2022 are tagged "after_year_2022" and re-stamped to Jan 1 2023.

    It also reads incoming system/user metadata and passes the user metadata
    through to the outgoing message, so the metadata round-trip can be asserted.
    """

    async def handler(self, keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        event_time = datum.event_time
        messages = Messages()

        # Read system metadata (read-only) to exercise the read path.
        for group in datum.system_metadata.groups():
            for key in datum.system_metadata.keys(group):
                datum.system_metadata.value(group, key)

        if event_time < january_first_2022:
            messages.append(Message.to_drop(event_time))
        elif event_time < january_first_2023:
            messages.append(
                Message(
                    value=val,
                    event_time=january_first_2022,
                    keys=keys,
                    tags=["within_year_2022"],
                    user_metadata=datum.user_metadata,
                )
            )
        else:
            messages.append(
                Message(
                    value=val,
                    event_time=january_first_2023,
                    keys=keys,
                    tags=["after_year_2022"],
                    user_metadata=datum.user_metadata,
                )
            )

        return messages


if __name__ == "__main__":
    grpc_server = SourceTransformAsyncServer(
        EventFilter(),
        sock_path=SOCK_PATH,
        server_info_file=SERVER_INFO,
    )
    grpc_server.start()
