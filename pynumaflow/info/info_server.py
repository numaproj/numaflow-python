import os

from pynumaflow import __version__
from pynumaflow.info.info_types import ServerInfo, EOF
import json


def get_sdk_version() -> str:
    """
    Return the pynumaflow SDK version
    """
    return __version__


def write(serv: ServerInfo, info_file):
    """
    Write the ServerInfo to a file , shared with the client (numa container).

    args:
        serv: The ServerInfo object to be shared
        info_file: the shared file path
    """
    data = serv.__dict__
    try:
        with open(info_file, "w+") as f:
            json.dump(data, f, ensure_ascii=False)
            f.write(EOF)
        return None
    except Exception as e:
        return e


#
def get_metadata_env(envs):
    """
    Extract the environment var value from the provided list,
    and assign them to the given key in the metadata

    args:
        envs: List of tuples (key, env_var)
    """
    meta = {}
    for env in envs:
        key, val = env
        res = os.getenv(val, None)
        if res:
            meta[key] = res
    return meta
