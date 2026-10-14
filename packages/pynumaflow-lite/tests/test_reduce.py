from pathlib import Path

import pytest

from _test_utils import run_python_server_with_rust_client
from pynumaflow_lite import reducer

SOCK_PATH = Path("/tmp/var/run/numaflow/reduce.sock")
SERVER_INFO = Path("/tmp/var/run/numaflow/reducer-server-info")

SCRIPTS = [
    "reduce_counter_class.py",
    "reduce_counter_func.py",
]


@pytest.mark.parametrize("script", SCRIPTS)
def test_python_reduce_server_and_rust_client(script: str, tmp_path: Path):
    run_python_server_with_rust_client(
        script=script,
        sock_path=SOCK_PATH,
        server_info_path=SERVER_INFO,
        rust_bin_name="test_reduce",
    )


def test_reduce_data_types_match_pythonic_api():
    datum = reducer.Datum(keys=["key"], value=b"value")
    assert datum.keys == ["key"]
    assert datum.value == b"value"
    assert datum.event_time
    assert "value=b'value'" in repr(datum)

    message = reducer.Message(b"value", keys=["key"], tags=["tag"])
    assert message.value == b"value"
    assert message.keys == ["key"]
    assert message.tags == ["tag"]
    assert message == reducer.Message(b"value", keys=["key"], tags=["tag"])
    assert repr(message) == "Message(value=b'value', keys=['key'], tags=['tag'])"
    assert reducer.Message.to_drop().tags == ["U+005C__DROP__"]

    assert not hasattr(reducer, "Messages")
    assert not hasattr(reducer.Message, "message_to_drop")
    assert not hasattr(reducer, "PyAsyncDatumStream")
