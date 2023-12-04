import datetime
import logging

from pynumaflow.sourcetransformer import Messages, Message, Datum, SourceTransformer

"""
This is a simple User Defined Function example which receives a message, applies the following
data transformation, and returns the message.
If the message event time is before year 2022, drop the message with event time unchanged.
If it's within year 2022, update the tag to "within_year_2022" and 
update the message event time to Jan 1st 2022.
Otherwise, (exclusively after year 2022), update the tag to "after_year_2022" and update the
message event time to Jan 1st 2023.
"""

january_first_2022 = datetime.datetime.fromtimestamp(1640995200)
january_first_2023 = datetime.datetime.fromtimestamp(1672531200)


def my_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    event_time = datum.event_time
    messages = Messages()

    if event_time < january_first_2022:
        logging.info("Got event time:%s, it is before 2022, so dropping", event_time)
        messages.append(Message.to_drop(event_time))
    elif event_time < january_first_2023:
        logging.info(
            "Got event time:%s, it is within year 2022, so forwarding to within_year_2022",
            event_time,
        )
        messages.append(
            Message(value=val, event_time=january_first_2022, tags=["within_year_2022"])
        )
    else:
        logging.info(
            "Got event time:%s, it is after year 2022, so forwarding to after_year_2022", event_time
        )
        messages.append(Message(value=val, event_time=january_first_2023, tags=["after_year_2022"]))

    return messages


if __name__ == "__main__":
    grpc_server = SourceTransformer(handler=my_handler)
    grpc_server.start()
