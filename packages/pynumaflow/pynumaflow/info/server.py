import os
from typing import Any

from pynumaflow import setup_logging
from pynumaflow.info.types import ServerInfo, EOF
import json
import logging

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)


def write(server_info: ServerInfo, info_file: str):
    """
    Write the ServerInfo to a file , shared with the client (numa container).

    args:
        serv: The ServerInfo object to be shared
        info_file: the shared file path
    """
    try:
        data = server_info.__dict__
        with open(info_file, "w+") as f:
            json.dump(data, f, ensure_ascii=False)
            f.write(EOF)
    except Exception as err:
        _LOGGER.critical("Could not write data to Info-Server %r", err, exc_info=True)
        raise err


def get_metadata_env(envs: list[tuple[str, str]]) -> dict[str, Any]:
    """
    Extract the environment var value from the provided list,
    and assign them to the given key in the metadata

    args:
        envs: List of tuples (key, env_var)
    """
    meta = {}
    for key, val in envs:
        res = os.getenv(val, None)
        if res:
            meta[key] = res
    return meta
