import os
import json
import shutil
import threading
import unittest
from pynumaflow.errors.errors import persist_critical_error, _persist_error_once
from pynumaflow.errors.errors import _persist_critical_error_to_file
from pynumaflow._constants import CONTAINER_TYPE, INTERNAL_ERROR


class TestErrorPersistence(unittest.TestCase):
    def setUp(self):
        """
        Set up temporary directories for tests.
        """
        self.test_dirs = ["/tmp/test_error_dir", "/tmp/test_dir"]

    def tearDown(self):
        """
        Clean up temporary directories after tests.
        """
        for dir_path in self.test_dirs:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)

    # Writes error details to a JSON file
    def test_writes_error_details_to_json_file(self):
        """
        Test that _persist_critical_error_to_file writes error details to a JSON file.
        """

        dir_path = self.test_dirs[0]

        error_code = "500"
        error_message = "Server Error"
        error_details = "An unexpected error occurred."

        _persist_critical_error_to_file(error_code, error_message, error_details, dir_path)

        container_dir = os.path.join(dir_path, CONTAINER_TYPE)
        self.assertTrue(os.path.exists(container_dir))

        # Debug: Check directory after the function call
        print(f"After: {os.listdir(container_dir)}")

        files = os.listdir(container_dir)
        self.assertEqual(len(files), 1)

        final_file_name = files[0]
        final_file_path = os.path.join(container_dir, final_file_name)

        with open(final_file_path) as f:
            data = json.load(f)

        self.assertEqual(data["code"], error_code)
        self.assertEqual(data["message"], error_message)
        self.assertEqual(data["details"], error_details)
        self.assertEqual(data["container"], CONTAINER_TYPE)
        self.assertTrue(isinstance(data["timestamp"], int))

    # Uses default error code if none provided
    def test_uses_default_error_code_if_none_provided(self):
        """
        Test that _persist_critical_error_to_file uses the default error code if none is provided.
        """
        dir_path = self.test_dirs[1]

        _persist_critical_error_to_file("", "Error Message", "Error Details", dir_path)

        container_dir = os.path.join(dir_path, "unknown-container")
        self.assertTrue(os.path.exists(container_dir))

        files = os.listdir(container_dir)
        self.assertEqual(len(files), 1)

        with open(os.path.join(container_dir, files[0])) as f:
            error_data = json.load(f)
            self.assertEqual(error_data["code"], INTERNAL_ERROR)

    def test_persist_critical_error_all_threads_fail(self):
        """
        Test that all threads fail when persist_critical_error is executed after the first call.
        """
        error_code = "testCode"
        error_message = "testMessage"
        error_details = "testDetails"

        # Set `done` to True to simulate that the critical error has already been persisted
        _persist_error_once.done = True

        try:
            # Set up threading
            num_threads = 10
            errors = []
            lock = threading.Lock()

            def thread_func():
                nonlocal errors
                result = persist_critical_error(error_code, error_message, error_details)
                with lock:
                    errors.append(result)

            # Create and start threads
            threads = []
            for _ in range(num_threads):
                thread = threading.Thread(target=thread_func)
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Count the number of failures
            fail_count = sum(
                1
                for error in errors
                if error is not None
                and "Persist critical error function has already been executed" in str(error)
            )

            # Assert that all threads failed
            self.assertEqual(
                fail_count,
                num_threads,
                f"Expected all {num_threads} threads to fail, but only {fail_count} failed",
            )
        finally:
            # Revert `done` back to False after the test
            _persist_error_once.done = False


if __name__ == "__main__":
    unittest.main()
