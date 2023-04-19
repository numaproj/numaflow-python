import os

from pynumaflow import __version__
from pynumaflow.info.info_types import ServerInfo, EOF
import json


def get_sdk_version():
    return __version__


def write(serv: ServerInfo, info_file):
    data = serv.__dict__
    try:
        with open(info_file, "w+") as f:
            json.dump(data, f, ensure_ascii=False)
            f.write(EOF)
        return None
    except Exception as e:
        return e


def get_metadata(envs):
    meta = {}
    for env in envs:
        key, val = env
        res = os.getenv(val, None)
        if res:
            meta[key] = res
    return meta
