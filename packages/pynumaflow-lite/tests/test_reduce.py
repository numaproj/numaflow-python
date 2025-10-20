from pathlib import Path

import pytest

from _test_utils import run_python_server_with_rust_client

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
