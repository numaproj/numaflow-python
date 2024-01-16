from pynumaflow.mapper import Datum, Messages, Message, Mapper


async def async_map_error_fn(keys: list[str], datum: Datum) -> Messages:
    raise ValueError("error invoking map")


class ExampleMap(Mapper):
    def handler(self, keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        msg = "payload:{} event_time:{} watermark:{}".format(
            val.decode("utf-8"),
            datum.event_time,
            datum.watermark,
        )
        val = bytes(msg, encoding="utf-8")
        messages = Messages()
        messages.append(Message(val, keys=keys))
        return messages


def map_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    msg = "payload:{} event_time:{} watermark:{}".format(
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )
    val = bytes(msg, encoding="utf-8")
    messages = Messages()
    messages.append(Message(val, keys=keys))
    return messages


async def async_err_map_handler(_: list[str], __: Datum) -> Messages:
    raise RuntimeError("Something is fishy!")


def err_map_handler(_: list[str], __: Datum) -> Messages:
    raise RuntimeError("Something is fishy!")
