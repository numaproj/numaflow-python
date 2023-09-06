from pynumaflow.sourcetransformer import Datum, Messages, Message
from tests.testing_utils import mock_new_event_time


def transform_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    msg = "payload:{} event_time:{} ".format(
        val.decode("utf-8"),
        datum.event_time,
    )
    val = bytes(msg, encoding="utf-8")
    messages = Messages()
    messages.append(Message(val, mock_new_event_time(), keys=keys))
    return messages


def err_transform_handler(_: list[str], __: Datum) -> Messages:
    raise RuntimeError("Something is fishy!")
