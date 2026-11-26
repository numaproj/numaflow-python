from pynumaflow_lite import batchmapper


def test_datum_metadata():
    datum = batchmapper.Datum(
        id="id-1",
        value=b"value",
        user_metadata={"user_group": {"user_key": b"user_value"}},
        system_metadata={"sys_group": {"sys_key": b"sys_value"}},
    )

    assert datum.user_metadata == {"user_group": {"user_key": b"user_value"}}
    assert datum.system_metadata == {"sys_group": {"sys_key": b"sys_value"}}
    assert "user_metadata={'user_group': {'user_key': b'user_value'}}" in repr(datum)


def test_datum_default_metadata():
    datum = batchmapper.Datum(id="id-1")

    assert datum.user_metadata == {}
    assert datum.system_metadata == {}


def test_message_user_metadata():
    message = batchmapper.Message(b"value", keys=["key"], user_metadata={"group": {"k": b"v"}})

    assert message.user_metadata == {"group": {"k": b"v"}}
    assert repr(message) == (
        "Message(value=b'value', keys=['key'], tags=None, user_metadata={'group': {'k': b'v'}}, nack_options=None)"
    )
    assert batchmapper.Message(b"value").user_metadata is None
    assert batchmapper.Message.to_drop().user_metadata is None
