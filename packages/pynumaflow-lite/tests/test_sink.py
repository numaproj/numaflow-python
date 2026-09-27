from pathlib import Path

import pytest

from _test_utils import run_python_server_with_rust_client
from pynumaflow_lite import sinker

SOCK_PATH = Path("/tmp/var/run/numaflow/sink.sock")
SERVER_INFO = Path("/tmp/var/run/numaflow/sinker-server-info")

SCRIPTS = [
    "sink_log.py",
    "sink_log_class.py",
]


@pytest.mark.parametrize("script", SCRIPTS)
def test_python_sink_server_and_rust_client(script: str, tmp_path: Path):
    run_python_server_with_rust_client(
        script=script,
        sock_path=SOCK_PATH,
        server_info_path=SERVER_INFO,
        rust_bin_name="test_sink",
    )


def test_sink_data_types_match_pythonic_api():
    datum = sinker.Datum(keys=["key"], value=b"value", id="id-1")
    assert datum.keys == ["key"]
    assert datum.value == b"value"
    assert datum.id == "id-1"
    assert datum.event_time
    assert datum.user_metadata == {}
    assert isinstance(datum.system_metadata, dict)
    assert "value=b'value'" in repr(datum)
    assert repr(sinker.Datum(id="id-2", value=b"\xff")).startswith("Datum(id='id-2', keys=[], value=b'\\xff'")
    with pytest.raises(TypeError):
        sinker.Datum(value=b"value")  # id is required

    message = sinker.Message(
        b"value",
        keys=["key"],
        user_metadata={"custom_info": {"version": b"1.0.0"}},
    )

    assert message.keys == ["key"]
    assert message.value == b"value"
    assert message.user_metadata["custom_info"]["version"] == b"1.0.0"
    assert message == sinker.Message(
        b"value",
        keys=["key"],
        user_metadata={"custom_info": {"version": b"1.0.0"}},
    )

    response = sinker.Response.success("id-1")
    assert response.id == "id-1"
    assert response.error is None
    assert response == sinker.Response.success("id-1")
    assert sinker.Response.failure("id-1", "boom").error == "boom"
    assert response.serve_response is None
    assert response.on_success_message is None

    serve = sinker.Response.serve("id-1", b"payload")
    assert serve.serve_response == b"payload"
    assert repr(serve) == "Response.serve(id='id-1', payload=b'payload')"

    on_success = sinker.Response.on_success("id-1", message)
    assert on_success.on_success_message == message
    assert repr(on_success) == (
        "Response.on_success(id='id-1', message=Message(value=b'value', keys=['key'], "
        "user_metadata={'custom_info': {'version': b'1.0.0'}}))"
    )

    nack = sinker.Response.nack("id-1", sinker.NackOptions(delay=5))
    assert repr(nack) == f"Response.nack(id='id-1', nack_options={nack.nack_options!r})"
    assert "Some(" not in repr(nack)
    assert not hasattr(sinker, "KeyValueGroup")
    assert not hasattr(sinker, "Responses")
    assert not hasattr(sinker, "PyAsyncDatumStream")
