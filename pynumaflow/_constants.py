MAP_SOCK_PATH = "/var/run/numaflow/map.sock"
MAP_STREAM_SOCK_PATH = "/var/run/numaflow/mapstream.sock"
REDUCE_SOCK_PATH = "/var/run/numaflow/reduce.sock"
SOURCE_TRANSFORMER_SOCK_PATH = "/var/run/numaflow/sourcetransform.sock"
SINK_SOCK_PATH = "/var/run/numaflow/sink.sock"
MULTIPROC_MAP_SOCK_PORT = 55551
MULTIPROC_MAP_SOCK_ADDR = "0.0.0.0"
SIDE_INPUT_SOCK_PATH = "/var/run/numaflow/sideinput.sock"

# TODO: need to make sure the DATUM_KEY value is the same as
# https://github.com/numaproj/numaflow-go/blob/main/pkg/function/configs.go#L6
WIN_START_TIME = "x-numaflow-win-start-time"
WIN_END_TIME = "x-numaflow-win-end-time"
MAX_MESSAGE_SIZE = 1024 * 1024 * 64
# TODO: None instead of "EOF" ?
STREAM_EOF = "EOF"
DELIMITER = ":"
DROP = "U+005C__DROP__"
