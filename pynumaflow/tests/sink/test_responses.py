import unittest
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
        self.assertEqual(
            "[Response(id='2', success=True, err=None), "
            "Response(id='3', success=False, err='RuntimeError encountered!'), "
            "Response(id='4', success=True, err=None)]",
            repr(self.resps),
        )

    def test_dumps(self):
        dump_str = self.resps.dumps()
        self.assertEqual(
            "[Response(id='2', success=True, err=None), "
            "Response(id='3', success=False, err='RuntimeError encountered!')]",
            dump_str,
        )


if __name__ == "__main__":
    unittest.main()
