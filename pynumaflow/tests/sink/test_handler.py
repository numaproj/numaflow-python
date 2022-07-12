import json
import tempfile
import unittest
from typing import List

import msgpack
from aiohttp.test_utils import AioHTTPTestCase

from pynumaflow._constants import APPLICATION_JSON, APPLICATION_MSG_PACK
from pynumaflow.decoder import msgpack_decoder
from pynumaflow.encoder import msgpack_encoding, NumaflowJSONEncoder
from pynumaflow.sink import HTTPSinkHandler, Responses, Response, Message


def udsink_handler(messages: List[Message], __) -> Responses:
    responses = Responses()
    for msg in messages:
        responses.append(Response.as_success(msg.id))
    return responses


class TestHTTPSinkHandler(AioHTTPTestCase):
    async def tearDownAsync(self) -> None:
        await self.handler.app.shutdown()

    async def get_application(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.handler = HTTPSinkHandler(udsink_handler, sock_path=tmp_dir)
            return self.handler.app

    async def test_handler_routes(self):
        async with self.client.request("GET", "/ready") as resp:
            self.assertEqual(resp.status, 204)

        dummy_data = [
            Message(id="1993", payload=b"random 1"),
            Message(id="1994", payload=b"random 2"),
        ]

        # with msgpack content
        msgpack_data = msgpack.dumps(dummy_data, default=msgpack_encoding)
        async with self.client.request(
            "POST", "/messages", data=msgpack_data, headers={"Content-Type": APPLICATION_MSG_PACK}
        ) as resp:
            self.assertEqual(resp.status, 200)
            binary_content = await resp.read()
        self.assertListEqual(
            [
                {"id": "1993", "success": True, "err": None},
                {"id": "1994", "success": True, "err": None},
            ],
            msgpack.unpackb(binary_content, object_pairs_hook=msgpack_decoder),
        )

        # with json content
        json_data = json.dumps(dummy_data, cls=NumaflowJSONEncoder, separators=(",", ":"))
        async with self.client.request(
            "POST", "/messages", data=json_data, headers={"Content-Type": APPLICATION_JSON}
        ) as resp:
            self.assertEqual(resp.status, 200)
            text = await resp.text()
        self.assertListEqual(
            [
                {"id": "1993", "success": True, "err": None},
                {"id": "1994", "success": True, "err": None},
            ],
            json.loads(text),
        )

        # without header
        json_data = json.dumps(dummy_data, cls=NumaflowJSONEncoder, separators=(",", ":"))
        async with self.client.request("POST", "/messages", data=json_data) as resp:
            self.assertEqual(resp.status, 500)


class TestHTTPSinkHandlerSingleton(unittest.TestCase):
    def test_singleton(self):
        handler_1 = HTTPSinkHandler(udsink_handler)
        handler_2 = HTTPSinkHandler(lambda x: x + 1)
        self.assertEqual(id(handler_1), id(handler_2))


if __name__ == "__main__":
    unittest.main()
