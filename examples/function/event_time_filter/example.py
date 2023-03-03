import datetime

from pynumaflow.function import MessageTs, MessageT, Datum, UserDefinedFunctionServicer

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


def my_handler(key: str, datum: Datum) -> MessageTs:
    val = datum.value
    event_time = datum.event_time
    messages = MessageTs()

    if event_time < january_first_2022:
        print(event_time)
        print("...dropping")
        messages.append(MessageT.to_drop)
    elif event_time < january_first_2023:
        print(event_time)
        print("...forwarding to within_year_2022")
        messages.append(MessageT.to_vtx("within_year_2022", val, january_first_2022))
    else:
        print(event_time)
        print("...forwarding to after_year_2022")
        messages.append(MessageT.to_vtx("after_year_2022", val, january_first_2023))

    return messages


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(mapt_handler=my_handler)
    grpc_server.start()
