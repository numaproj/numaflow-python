import os
import unittest
from unittest import mock

from pynumaflow.info.info_server import get_sdk_version

from pynumaflow.info import info_server, info_types
from pynumaflow.info.info_types import ServerInfo
from pynumaflow.tests.function.testing_utils import read_info_server


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


class TestInfoServer(unittest.TestCase):
    @mockenv(NUMAFLOW_CPU_LIMIT="3")
    def setUp(self) -> None:
        self.serv_uds = ServerInfo(
            protocol=info_types.UDS,
            language=info_types.PYTHON,
            version=get_sdk_version(),
            metadata=info_server.get_metadata_env(info_types.metadata_envs),
        )

    def test_empty_write_info(self):
        test_file = "/tmp/test_info_server"
        ret = info_server.write(None, test_file)
        self.assertIsNotNone(ret)

    def test_success_write_info(self):
        test_file = "/tmp/test_info_server"
        ret = info_server.write(self.serv_uds, test_file)
        self.assertIsNone(ret)
        file_data = read_info_server(info_file=test_file)
        self.assertEqual(file_data["metadata"]["CPU_LIMIT"], "3")
        self.assertEqual(file_data["protocol"], "uds")
        self.assertEqual(file_data["language"], "python")

    def test_metadata_env(self):
        test_file = "/tmp/test_info_server"
        ret = info_server.write(self.serv_uds, test_file)
        self.assertIsNone(ret)

    def test_invalid_input(self):
        with self.assertRaises(ValueError):
            ServerInfo()

    def test_file_new(self):
        test_file = "/tmp/test_info_server"
        exists = os.path.isfile(test_file)
        if exists:
            os.remove(test_file)
        ret = info_server.write(self.serv_uds, test_file)
        self.assertIsNone(ret)
