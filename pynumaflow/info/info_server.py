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
        return True, None
    except Exception as e:
        return False, e


def get_metadata(envs):
    meta = {}
    for env in envs:
        key, val = env
        print(val)
        res = os.getenv(val, None)
        if res:
            meta[key] = res
    return meta


def read(info_file):
    f = open(info_file, "r")
    retry = 10
    res = f.read()
    a, b = is_ready(res)
    while (a is not True) and retry > 0:
        a, b = is_ready(res)

    a, b = is_ready(res)
    if a:
        res = json.loads(b)
        print(res)
    else:
        print("NO")


def is_ready(res, eof=EOF):
    if len(res) < len(eof):
        return False
    len_diff = len(res) - len(eof)
    last_char = res[len_diff:]
    if last_char == EOF:
        data = res[:len_diff]
        return True, data
    return False, None
