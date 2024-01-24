import unittest


from pynumaflow.info.server import get_sdk_version
from pynumaflow.info.types import Protocol
from pynumaflow.mapper import Datum, Messages, Message

from pynumaflow.shared.server import write_info_file
from tests.testing_utils import read_info_server


def map_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    msg = "payload:{} event_time:{} watermark:{}".format(
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )
    val = bytes(msg, encoding="utf-8")
    messages = Messages()
    messages.append(Message(val, keys=keys))
    return messages


class TestSharedUtils(unittest.TestCase):
    def test_write_info_file(self):
        """
        Test write_info_file function
        Write data to the info file and read it back to verify
        """
        info_file = "/tmp/test_info_server"
        ret = write_info_file(info_file=info_file, protocol=Protocol.UDS)
        self.assertIsNone(ret)
        file_data = read_info_server(info_file=info_file)
        self.assertEqual(file_data["protocol"], "uds")
        self.assertEqual(file_data["language"], "python")
        self.assertEqual(file_data["version"], get_sdk_version())
