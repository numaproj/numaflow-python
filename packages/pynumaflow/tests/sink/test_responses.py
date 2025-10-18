import unittest
from collections.abc import Iterator

from pynumaflow.sinker import Response, Responses, Sinker, Datum


class TestResponse(unittest.TestCase):
    def test_as_success(self):
        succ_response = Response.as_success("2")
        self.assertTrue(succ_response.success)

    def test_as_failure(self):
        _response = Response.as_failure("3", "RuntimeError encountered!")
        self.assertFalse(_response.success)

    def test_as_fallback(self):
        _response = Response.as_fallback("4")
        self.assertFalse(_response.success)
        self.assertTrue(_response.fallback)


class TestResponses(unittest.TestCase):
    def setUp(self) -> None:
        self.resps = Responses(
            Response.as_success("2"),
            Response.as_failure("3", "RuntimeError encountered!"),
            Response.as_fallback("5"),
        )

    def test_responses(self):
        self.resps.append(Response.as_success("4"))
        self.assertEqual(4, len(self.resps))

        for resp in self.resps:
            self.assertIsInstance(resp, Response)

        self.assertEqual(self.resps[0].id, "2")
        self.assertEqual(self.resps[1].id, "3")
        self.assertEqual(self.resps[2].id, "5")
        self.assertEqual(self.resps[3].id, "4")

        self.assertEqual(
            "[Response(id='2', success=True, err=None, fallback=False), "
            "Response(id='3', success=False, err='RuntimeError encountered!', fallback=False), "
            "Response(id='5', success=False, err=None, fallback=True), "
            "Response(id='4', success=True, err=None, fallback=False)]",
            repr(self.resps),
        )


class ExampleSinkClass(Sinker):
    def handler(self, datums: Iterator[Datum]) -> Responses:
        results = Responses()
        results.append(Response.as_success("test_message"))
        return results


class TestSinkClass(unittest.TestCase):
    def setUp(self) -> None:
        # Create a map class instance
        self.sinker_instance = ExampleSinkClass()

    def test_sink_class_call(self):
        """Test that the __call__ functionality for the class works,
        ie the class instance can be called directly to invoke the handler function
        """
        # make a call to the class directly
        ret = self.sinker_instance(None)
        self.assertEqual("test_message", ret[0].id)
        # make a call to the handler
        ret_handler = self.sinker_instance.handler(None)
        # Both responses should be equal
        self.assertEqual(ret[0], ret_handler[0])


if __name__ == "__main__":
    unittest.main()
