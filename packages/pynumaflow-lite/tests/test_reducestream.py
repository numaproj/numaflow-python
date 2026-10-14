from pathlib import Path

import pytest

from _test_utils import run_python_server_with_rust_client
from pynumaflow_lite import reducestreamer

SOCK_PATH = Path("/tmp/var/run/numaflow/reducestream.sock")
SERVER_INFO = Path("/tmp/var/run/numaflow/reducestreamer-server-info")

SCRIPTS = [
    "reducestream_counter.py",
    "reducestream_counter_func.py",
]


@pytest.mark.parametrize("script", SCRIPTS)
def test_python_reducestream_server_and_rust_client(script: str, tmp_path: Path):
    run_python_server_with_rust_client(
        script=script,
        sock_path=SOCK_PATH,
        server_info_path=SERVER_INFO,
        rust_bin_name="test_reducestream",
    )


def test_reducestream_data_types_match_pythonic_api():
    datum = reducestreamer.Datum(keys=["key"], value=b"value")
    assert datum.keys == ["key"]
    assert datum.event_time

    message = reducestreamer.Message(b"value", keys=["key"])
    assert message == reducestreamer.Message(b"value", keys=["key"])
    assert reducestreamer.Message.to_drop().tags == ["U+005C__DROP__"]

    assert not hasattr(reducestreamer.Message, "message_to_drop")
    assert not hasattr(reducestreamer, "PyAsyncDatumStream")
