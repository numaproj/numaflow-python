from pathlib import Path

import pytest

from _test_utils import run_python_server_with_rust_client

SOCK_PATH = Path("/tmp/var/run/numaflow/batchmap.sock")
SERVER_INFO = Path("/tmp/var/run/numaflow/mapper-server-info")

SCRIPTS = [
    "batchmap_cat.py",
    "batchmap_cat_class.py",
]


@pytest.mark.parametrize("script", SCRIPTS)
def test_python_batch_server_and_rust_client(script: str, tmp_path: Path):
    run_python_server_with_rust_client(
        script=script,
        sock_path=SOCK_PATH,
        server_info_path=SERVER_INFO,
        rust_bin_name="test_batchmap",
    )
