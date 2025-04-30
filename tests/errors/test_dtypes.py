import unittest
from pynumaflow.errors._dtypes import _RuntimeErrorEntry


class TestRuntimeErrorEntry(unittest.TestCase):
    def test_runtime_error_entry_initialization(self):
        """
        Test that _RuntimeErrorEntry initializes correctly with given values.
        """
        container = "test-container"
        timestamp = 1680700000
        code = "500"
        message = "Test error message"
        details = "Test error details"

        error_entry = _RuntimeErrorEntry(container, timestamp, code, message, details)

        self.assertEqual(error_entry.container, container)
        self.assertEqual(error_entry.timestamp, timestamp)
        self.assertEqual(error_entry.code, code)
        self.assertEqual(error_entry.message, message)
        self.assertEqual(error_entry.details, details)

    def test_runtime_error_entry_to_dict(self):
        """
        Test that _RuntimeErrorEntry converts to a dictionary correctly.
        """
        container = "test-container"
        timestamp = 1680700000
        code = "500"
        message = "Test error message"
        details = "Test error details"

        error_entry = _RuntimeErrorEntry(container, timestamp, code, message, details)
        error_dict = error_entry.to_dict()

        expected_dict = {
            "container": container,
            "timestamp": timestamp,
            "code": code,
            "message": message,
            "details": details,
        }

        self.assertEqual(error_dict, expected_dict)

    def test_runtime_error_entry_empty_values(self):
        """
        Test that _RuntimeErrorEntry handles empty values correctly.
        """
        container = ""
        timestamp = 0
        code = ""
        message = ""
        details = ""

        error_entry = _RuntimeErrorEntry(container, timestamp, code, message, details)
        error_dict = error_entry.to_dict()

        expected_dict = {
            "container": container,
            "timestamp": timestamp,
            "code": code,
            "message": message,
            "details": details,
        }

        self.assertEqual(error_dict, expected_dict)


if __name__ == "__main__":
    unittest.main()
