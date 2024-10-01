import os
import unittest
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


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


class TestInfoServer(unittest.TestCase):
    @mockenv(NUMAFLOW_CPU_LIMIT="3")
    def setUp(self) -> None:
        self.serv_uds = ServerInfo.get_default_server_info()
        self.serv_uds.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Sourcer]
        self.serv_uds.metadata = get_metadata_env(envs=METADATA_ENVS)

    def test_empty_write_info(self):
        test_file = "/tmp/test_info_server"
        with self.assertRaises(Exception):
            info_server_write(server_info=None, info_file=test_file)

    def test_success_write_info(self):
        test_file = "/tmp/test_info_server"
        ret = info_server_write(server_info=self.serv_uds, info_file=test_file)
        self.assertIsNone(ret)
        file_data = read_info_server(info_file=test_file)
        self.assertEqual(file_data["metadata"]["CPU_LIMIT"], "3")
        self.assertEqual(file_data["protocol"], "uds")
        self.assertEqual(file_data["language"], "python")
        self.assertEqual(file_data["minimum_numaflow_version"], "1.3.0-z")

    def test_metadata_env(self):
        test_file = "/tmp/test_info_server"
        ret = info_server_write(server_info=self.serv_uds, info_file=test_file)
        self.assertIsNone(ret)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            ServerInfo()

    def test_file_new(self):
        test_file = "/tmp/test_info_server"
        exists = os.path.isfile(path=test_file)
        if exists:
            os.remove(test_file)
        ret = info_server_write(server_info=self.serv_uds, info_file=test_file)
        self.assertIsNone(ret)
