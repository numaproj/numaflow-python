import os
import unittest
from unittest import mock

from pynumaflow.tests.function.testing_utils import read_info_server
from pynumaflow.info.server import (
    get_sdk_version,
    write as info_server_write,
    get_metadata_env,
)
from pynumaflow.info.types import (
    ServerInfo,
    Protocol,
    Language,
    METADATA_ENVS,
)


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


class TestInfoServer(unittest.TestCase):
    @mockenv(NUMAFLOW_CPU_LIMIT="3")
    def setUp(self) -> None:
        self.serv_uds = ServerInfo(
            protocol=Protocol.UDS,
            language=Language.PYTHON,
            version=get_sdk_version(),
            metadata=get_metadata_env(envs=METADATA_ENVS),
        )

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
