from pynumaflow_lite import mapper


class SimpleCat(mapper.Mapper):
    async def handler(self, datum: mapper.Datum) -> list[mapper.Message]:
        if datum.value == b"bad world":
            return [mapper.Message.to_drop()]
        return [mapper.Message(datum.value, keys=datum.keys)]


if __name__ == "__main__":
    mapper.MapAsyncServer(SimpleCat()).run()
