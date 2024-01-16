from pynumaflow.mapper import Messages, Message, Datum, MapServer, Mapper


class Flatmap(Mapper):
    def handler(self, keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        strs = val.decode("utf-8").split(",")
        messages = Messages()
        if len(strs) == 0:
            messages.append(Message.to_drop())
            return messages
        for s in strs:
            messages.append(Message(str.encode(s)))
        return messages


if __name__ == "__main__":
    grpc_server = MapServer(Flatmap())
    grpc_server.start()
