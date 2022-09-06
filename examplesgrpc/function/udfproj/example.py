import os

from pynumaflowgrpc.function import HTTPHandler
from pynumaflowgrpc.function import Messages, Message
import json


os.environ["NUMAFLOW_UDF_CONTENT_TYPE"] = "application/json"


def my_handler(_, msg: bytes, __):
    messages = Messages()
    r = json.loads(msg.decode("utf-8"))
    if r.get("number") is not None:
        try:
            print("number found", r)
            r["number"] *= 2
            messages.append(Message.to_all(json.dumps(r).encode()))
        except Exception as ex:
            print(f"dropped {r}, err: {repr(ex)}")
            messages.append(Message.to_drop())
    else:
        print("number missing", r)
        messages.append(Message.to_drop())
    return messages


if __name__ == "__main__":
    handler = HTTPHandler(my_handler)
    handler.start()
