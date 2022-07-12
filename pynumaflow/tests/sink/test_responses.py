import unittest

from pynumaflow._constants import APPLICATION_JSON, APPLICATION_MSG_PACK
from pynumaflow.exceptions import MarshalError
from pynumaflow.sink import Response, Responses


class TestResponse(unittest.TestCase):
    def test_as_success(self):
        succ_response = Response.as_success("2")
        self.assertTrue(succ_response.success)

    def test_as_failure(self):
        succ_response = Response.as_failure("3", "RuntimeError encountered!")
        self.assertFalse(succ_response.success)


class TestResponses(unittest.TestCase):
    def setUp(self) -> None:
        self.resps = Responses(
            Response.as_success("2"), Response.as_failure("3", "RuntimeError encountered!")
        )

    def test_responses(self):
        self.resps.append(Response.as_success("4"))
        self.assertEqual(3, len(self.resps.items()))

    def test_dumps_json(self):
        resps_json = self.resps.dumps(APPLICATION_JSON)
        self.assertTrue(resps_json)

    def test_dumps_msgpack(self):
        resps_msgpack = self.resps.dumps(APPLICATION_MSG_PACK)
        self.assertTrue(resps_msgpack)

    def test_dumps_err(self):
        with self.assertRaises(MarshalError):
            self.resps.dumps("random_content_type")


if __name__ == "__main__":
    unittest.main()
