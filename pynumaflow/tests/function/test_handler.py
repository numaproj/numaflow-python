import json
import os
import tempfile
import unittest

from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase

from pynumaflow._constants import NUMAFLOW_UDF_CONTENT_TYPE, APPLICATION_JSON
from pynumaflow.function.handler import HTTPHandler
from pynumaflow.function._dtypes import Message, Messages


def mock_message():
    msg = {"test": "handler"}
    msg = json.dumps(msg).encode("utf-8")
    return msg


def mock_json():
    msg = {"test": "handler"}
    msg = json.dumps(msg)
    return msg


def mock_success_response():
    v = mock_message()
    msgs = Messages.as_forward_all(v)
    v = msgs.dumps(APPLICATION_JSON)

    return web.Response(body=v, status=200, content_type=APPLICATION_JSON)


def dummy_handler(_, val, __):
    print(val)
    msgs = Messages()
    msgs.append(Message.to_all(val))
    return msgs


class TestHTTPHandler(AioHTTPTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[NUMAFLOW_UDF_CONTENT_TYPE] = APPLICATION_JSON

    async def get_application(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.handler = HTTPHandler(dummy_handler, sock_path=tmp_dir)
            return self.handler.app

    async def test_handler_routes(self):
        async with self.client.request("GET", "/ready") as resp:
            self.assertEqual(resp.status, 204)

        async with self.client.request("POST", "/messages", data='{"a": "b"}', headers={}) as resp:
            self.assertEqual(resp.status, 200)
            text = await resp.text()
        self.assertTrue(text)


class TestHTTPHandlerSingleton(unittest.TestCase):
    def test_singleton(self):
        handler_1 = HTTPHandler(dummy_handler)
        handler_2 = HTTPHandler(lambda x: x + 1)
        self.assertEqual(id(handler_1), id(handler_2))


if __name__ == "__main__":
    unittest.main()
