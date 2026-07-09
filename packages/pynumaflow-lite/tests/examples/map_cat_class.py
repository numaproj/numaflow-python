from pynumaflow_lite import mapper


class SimpleCat(mapper.Mapper):
    async def handler(self, datum: mapper.Datum) -> list[mapper.Message]:
        # Read system metadata (read-only)
        print(f"System metadata groups: {list(datum.system_metadata)}")
        for group, key_values in datum.system_metadata.items():
            for key, value in key_values.items():
                print(f"  System[{group}][{key}] = {value}")

        # Read user metadata
        print(f"User metadata groups: {list(datum.user_metadata)}")
        for group, key_values in datum.user_metadata.items():
            for key, value in key_values.items():
                print(f"  User[{group}][{key}] = {value}")

        if datum.value == b"bad world":
            return [mapper.Message.to_drop()]

        user_metadata = {
            "processing": {
                "handler": b"map_cat_class",
                "msg_length": str(len(datum.value)).encode(),
            }
        }
        return [mapper.Message(datum.value, keys=datum.keys, user_metadata=user_metadata)]


if __name__ == "__main__":
    mapper.MapAsyncServer(
        SimpleCat(),
        sock_file="/tmp/var/run/numaflow/map.sock",
        server_info_file="/tmp/var/run/numaflow/mapper-server-info",
    ).run()
