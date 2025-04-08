import os
import json
import threading
import time
from pynumaflow._constants import (
    CONTAINER_TYPE,
    RUNTIME_APPLICATION_ERRORS_PATH,
    CURRENT_CRITICAL_ERROR_FILE,
    INTERNAL_ERROR_CODE,
)
from pynumaflow.errors._dtypes import _RuntimeErrorEntry
from typing import Union


class _PersistErrorOnce:
    """Ensures that the persist_critical_error function is executed only once."""

    def __init__(self):
        self.done = False
        self.lock = threading.Lock()

    def execute(self, func, *args, **kwargs):
        with self.lock:
            if self.done:
                raise RuntimeError("Persist critical error function has already been executed.")
            self.done = True
            return func(*args, **kwargs)


_persist_error_once = _PersistErrorOnce()


def persist_critical_error(
    error_code: str, error_message: str, error_details: str
) -> Union[RuntimeError, None]:
    """
    Persists a critical error to a file. This function will only execute once.
    Logs the error if persisting to the file fails.
    Returns None if successful, or raises RuntimeError if already executed.
    """
    try:
        _persist_error_once.execute(
            _persist_critical_error_to_file,
            error_code,
            error_message,
            error_details,
            RUNTIME_APPLICATION_ERRORS_PATH,
        )
    except RuntimeError as e:
        return e
    except Exception as e:
        print(f"Error in persisting critical error: {e}")
    return None


def _persist_critical_error_to_file(
    error_code: str, error_message: str, error_details: str, dir_path: str
):
    """Internal function to persist a critical error to a file."""

    os.makedirs(dir_path, mode=0o777, exist_ok=True)
    container_dir = os.path.join(dir_path, CONTAINER_TYPE)
    os.makedirs(container_dir, mode=0o777, exist_ok=True)

    current_file_path = os.path.join(container_dir, CURRENT_CRITICAL_ERROR_FILE)
    error_code = error_code or INTERNAL_ERROR_CODE
    current_timestamp = int(time.time())

    runtime_error_entry = _RuntimeErrorEntry(
        container=CONTAINER_TYPE,
        timestamp=current_timestamp,
        code=error_code,
        message=error_message,
        details=error_details,
    )

    with open(current_file_path, "w") as f:
        json.dump(runtime_error_entry.to_dict(), f)

    final_file_name = f"{current_timestamp}-udf.json"
    final_file_path = os.path.join(container_dir, final_file_name)
    os.rename(current_file_path, final_file_path)
