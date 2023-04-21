from enum import Enum
from typing import Dict

# Constants for using in the info-server
# Need to keep consistent with all SDKs and client
SERVER_INFO_FILE_PATH = "/var/run/numaflow/server-info"
EOF = "U+005C__END__"

# Env variables to be passed in the info server metadata.
# These need to be accessed in the client using the same key.
# Format - (key, env_var)
metadata_envs = [("CPU_LIMIT", "NUMAFLOW_CPU_LIMIT")]


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


class ServerInfo:
    """
    ServerInfo is used for the gRPC server to provide the information such as protocol,
    sdk version, language, metadata to the client.

    Args:
        protocol: Protocol to use (UDS or TCP)
        language: Language used by the server(Python, Golang, Java)
        version: Numaflow sdk version used by the server
        metadata: Any additional information to be provided (env vars)
    """

    def __init__(
        self,
        protocol: Protocol = None,
        language: Language = None,
        version: str = None,
        metadata: Dict = None,
    ):
        if protocol is None or language is None or version is None:
            raise ValueError("Need to specify mandatory details in ServerInfo")
        self.protocol = protocol
        self.language = language
        self.version = version
        self.metadata = metadata
