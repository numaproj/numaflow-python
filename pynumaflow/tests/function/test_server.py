import json
import os
import tempfile
import unittest

from pynumaflow.function.server import UserDefinedFunctionServicer
from pynumaflow.function._dtypes import Message, Messages


def mock_message():
    msg = {"test_mock_message"}
    return msg


if __name__ == "__main__":
    unittest.main()
