from pathlib import Path

import pytest

from pynumaflow.sourcetransformer import Datum, Messages, Message, SourceTransformer
from pynumaflow.sourcetransformer.async_server import SourceTransformAsyncServer
from tests._test_utils import run_python_server_with_rust_client
from tests.testing_utils import mock_new_event_time

pytestmark = pytest.mark.integration

SOCK_PATH = Path("/tmp/var/run/numaflow/sourcetransform.sock")
SERVER_INFO = Path("/tmp/var/run/numaflow/sourcetransformer-server-info")

SCRIPTS = [
    "sourcetransform_event_filter.py",
]


@pytest.mark.parametrize("script", SCRIPTS)
def test_python_server_and_rust_client(script: str):
    """End-to-end test: start the Rust-backed async server (driven by the public
    pynumaflow API) and exercise it with a compiled Rust tonic client.

    The Rust server uses a tonic gRPC server, which the Python ``grpcio`` client
    cannot interoperate with over a Unix socket, so we drive it from Rust.
    """
    run_python_server_with_rust_client(
        script=script,
        sock_path=SOCK_PATH,
        server_info_path=SERVER_INFO,
        rust_bin_name="test_sourcetransform",
    )


class SimpleAsyncSourceTrn(SourceTransformer):
    async def handler(self, keys: list[str], datum: Datum) -> Messages:
        messages = Messages()
        messages.append(Message(datum.value, mock_new_event_time(), keys=keys))
        return messages


def test_invalid_input():
    with pytest.raises(TypeError):
        SourceTransformAsyncServer()


@pytest.mark.parametrize(
    "max_threads_arg,expected",
    [
        (32, 16),  # max cap at 16
        (5, 5),  # use argument provided
        (None, 4),  # defaults to 4
    ],
)
def test_max_threads(max_threads_arg, expected):
    handle = SimpleAsyncSourceTrn()
    kwargs = {"source_transform_instance": handle}
    if max_threads_arg is not None:
        kwargs["max_threads"] = max_threads_arg
    server = SourceTransformAsyncServer(**kwargs)
    assert server.max_threads == expected
