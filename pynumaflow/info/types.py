from dataclasses import dataclass, field
from enum import Enum

# Constants for using in the info-server
# Specify the minimum Numaflow version required by the current SDK version
MINIMUM_NUMAFLOW_VERSION = "1.2.0-rc4"
# Need to keep consistent with all SDKs and client
EOF = "U+005C__END__"

# Env variables to be passed in the info server metadata.
# These need to be accessed in the client using the same key.
# Format - (key, env_var)
METADATA_ENVS = [("CPU_LIMIT", "NUMAFLOW_CPU_LIMIT")]


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
