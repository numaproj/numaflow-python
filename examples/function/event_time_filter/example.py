import datetime
import logging
from typing import List

from pynumaflow.function import MessageTs, MessageT, Datum, Server

"""
This is a simple User Defined Function example which receives a message, applies the following
data transformation, and returns the message.
If the message event time is before year 2022, drop the message. If it's within year 2022, update
the key to "within_year_2022" and update the message event time to Jan 1st 2022.
Otherwise, (exclusively after year 2022), update the key to "after_year_2022" and update the
message event time to Jan 1st 2023.
"""

january_first_2022 = datetime.datetime.fromtimestamp(1640995200)
january_first_2023 = datetime.datetime.fromtimestamp(1672531200)


def my_handler(keys: List[str], datum: Datum) -> MessageTs:
    val = datum.value
    event_time = datum.event_time
    messages = MessageTs()

    if event_time < january_first_2022:
        logging.info("Got event time:%s, it is before 2022, so dropping", event_time)
        messages.append(MessageT.to_drop())
    elif event_time < january_first_2023:
        logging.info(
            "Got event time:%s, it is within year 2022, so forwarding to within_year_2022",
            event_time,
        )
        messages.append(
            MessageT(value=val, event_time=january_first_2022, keys=["within_year_2022"])
        )
    else:
        logging.info(
            "Got event time:%s, it is after year 2022, so forwarding to after_year_2022", event_time
        )
        messages.append(
            MessageT(value=val, event_time=january_first_2023, keys=["after_year_2022"])
        )

    return messages


if __name__ == "__main__":
    grpc_server = Server(mapt_handler=my_handler)
    grpc_server.start()
