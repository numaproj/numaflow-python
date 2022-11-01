FUNCTION_SOCK_PATH = "/var/run/numaflow/function.sock"
SINK_SOCK_PATH = "/var/run/numaflow/udsink.sock"
# TODO: need to make sure the DATUM_KEY value is the same as
# https://github.com/numaproj/numaflow-go/blob/main/pkg/function/configs.go#L6
DATUM_KEY = "x-numaflow-datum-key"
MAX_MESSAGE_SIZE = 1024 * 1024 * 4
