FUNCTION_SOCK_PATH = "/var/run/numaflow/function.sock"
MULTIPROC_FUNCTION_SOCK_PORT = 55551
MULTIPROC_FUNCTION_SOCK_ADDR = "0.0.0.0"

SINK_SOCK_PATH = "/var/run/numaflow/udsink.sock"
# TODO: need to make sure the DATUM_KEY value is the same as
# https://github.com/numaproj/numaflow-go/blob/main/pkg/function/configs.go#L6
WIN_START_TIME = "x-numaflow-win-start-time"
WIN_END_TIME = "x-numaflow-win-end-time"
MAX_MESSAGE_SIZE = 1024 * 1024 * 4
# TODO: None instead of "EOF" ?
STREAM_EOF = "EOF"
DELIMITER = ":"
