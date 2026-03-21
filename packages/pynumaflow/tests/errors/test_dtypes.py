from pynumaflow.errors._dtypes import _RuntimeErrorEntry


def test_runtime_error_entry_initialization():
    """
    Test that _RuntimeErrorEntry initializes correctly with given values.
    """
    container = "test-container"
    timestamp = 1680700000
    code = "500"
    message = "Test error message"
    details = "Test error details"

    error_entry = _RuntimeErrorEntry(container, timestamp, code, message, details)

    assert error_entry.container == container
    assert error_entry.timestamp == timestamp
    assert error_entry.code == code
    assert error_entry.message == message
    assert error_entry.details == details


def test_runtime_error_entry_to_dict():
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

    assert error_dict == expected_dict


def test_runtime_error_entry_empty_values():
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

    assert error_dict == expected_dict
