import os
import pytest
from unittest import mock

from tests.testing_utils import read_info_server
from pynumaflow.info.server import (
    write as info_server_write,
    get_metadata_env,
)
from pynumaflow.info.types import (
    ServerInfo,
    METADATA_ENVS,
    MINIMUM_NUMAFLOW_VERSION,
    ContainerType,
)


@pytest.fixture()
def serv_uds():
    with mock.patch.dict(os.environ, {"NUMAFLOW_CPU_LIMIT": "3"}):
        s = ServerInfo.get_default_server_info()
        s.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Sourcer]
        s.metadata = get_metadata_env(envs=METADATA_ENVS)
    return s


def test_empty_write_info():
    test_file = "/tmp/test_info_server"
    with pytest.raises(Exception):
        info_server_write(server_info=None, info_file=test_file)


def test_success_write_info(serv_uds):
    test_file = "/tmp/test_info_server"
    ret = info_server_write(server_info=serv_uds, info_file=test_file)
    assert ret is None
    file_data = read_info_server(info_file=test_file)
    assert file_data["metadata"]["CPU_LIMIT"] == "3"
    assert file_data["protocol"] == "uds"
    assert file_data["language"] == "python"
    assert file_data["minimum_numaflow_version"] == "1.4.0-z"


def test_metadata_env(serv_uds):
    test_file = "/tmp/test_info_server"
    ret = info_server_write(server_info=serv_uds, info_file=test_file)
    assert ret is None


def test_invalid_input():
    with pytest.raises(TypeError):
        ServerInfo()


def test_file_new(serv_uds):
    test_file = "/tmp/test_info_server"
    exists = os.path.isfile(path=test_file)
    if exists:
        os.remove(test_file)
    ret = info_server_write(server_info=serv_uds, info_file=test_file)
    assert ret is None
