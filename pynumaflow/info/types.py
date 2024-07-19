import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from importlib.metadata import version
from typing import TypeVar

from pynumaflow import setup_logging

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

# Constants for using in the info-server
# Specify the minimum Numaflow version required by the current SDK version
MINIMUM_NUMAFLOW_VERSION = "1.2.0-rc4"
# Need to keep consistent with all SDKs and client
EOF = "U+005C__END__"

# Env variables to be passed in the info server metadata.
# These need to be accessed in the client using the same key.
# Format - (key, env_var)
METADATA_ENVS = [("CPU_LIMIT", "NUMAFLOW_CPU_LIMIT")]

# Metadata keys

# MAP_MODE_KEY is used in the server info object's metadata to indicate which map mode is enabled
MAP_MODE_KEY = "MAP_MODE"
# MULTIPROC_KEY is the field used to indicate that Multiproc map mode is enabled
# The value contains the number of servers spawned.
MULTIPROC_KEY = "MULTIPROC"

SI = TypeVar("SI", bound="ServerInfo")


class Protocol(str, Enum):
    """
    Enumerate grpc server connection protocol.
    """

    UDS = "uds"
    TCP = "tcp"


class Language(str, Enum):
    """
    Enumerate Numaflow SDK language.
    """

    GO = "go"
    PYTHON = "python"
    JAVA = "java"


class MapMode(str, Enum):
    """
    Enumerate Map Mode to be enabled
    """

    UnaryMap = "unary-map"
    StreamMap = "stream-map"
    BatchMap = "batch-map"


@dataclass
class ServerInfo:
    """
    ServerInfo is used for the gRPC server to provide the information such as protocol,
    sdk version, language, metadata to the client.
    Args:
        protocol: Protocol to use (UDS or TCP)
        language: Language used by the server(Python, Golang, Java)
        minimum_numaflow_version: lower bound for the supported Numaflow version
        version: Numaflow sdk version used by the server
        metadata: Any additional information to be provided (env vars)
    """

    protocol: Protocol
    language: Language
    minimum_numaflow_version: str
    version: str
    metadata: dict = field(default_factory=dict)

    @classmethod
    def get_default_server_info(cls) -> SI:
        serv_info = ServerInfo(
            protocol=Protocol.UDS,
            language=Language.PYTHON,
            minimum_numaflow_version=MINIMUM_NUMAFLOW_VERSION,
            version=get_sdk_version(),
            metadata=dict(),
        )
        return serv_info


def get_sdk_version() -> str:
    """
    Return the pynumaflow SDK version
    """
    try:
        return version("pynumaflow")
    except Exception as e:
        # Adding this to handle the case for local test/CI where pynumaflow
        # will not be installed as a package
        _LOGGER.error("Could not read SDK version %r", e, exc_info=True)
        return ""
