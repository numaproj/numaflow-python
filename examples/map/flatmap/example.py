from pynumaflow.mapper import Messages, Message, Datum, MapServer, MapperClass


class Flatmap(MapperClass):
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
    flatmap_instance = Flatmap()
    grpc_server = MapServer(mapper_instance=flatmap_instance)
    grpc_server.start()
