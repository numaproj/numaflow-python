from typing import NewType

SERVER_INFO_FILE_PATH = "/var/run/numaflow/server-info"
UDS = "uds"
TCP = "tcp"

Go = "go"
Python = "python"
Java = "java"

EOF = "U+005C__END__"

metadata_envs = [("CPU_LIMIT", "NUMAFLOW_CPU_LIMIT")]
Language = NewType("Language", str)
Protocol = NewType("Protocol", str)


class ServerInfo:
    def __init__(self, protocol: Protocol, language: Language, version: str, metadata: {} = None):
        self.protocol = protocol
        self.language = language
        self.version = version
        self.metadata = metadata
