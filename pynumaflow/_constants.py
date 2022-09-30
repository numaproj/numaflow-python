APPLICATION_JSON = "application/json"
APPLICATION_MSG_PACK = "application/msgpack"
NUMAFLOW_UDF_CONTENT_TYPE = "NUMAFLOW_UDF_CONTENT_TYPE"
NUMAFLOW_MESSAGE_KEY = "x-numa-message-key"
FUNCTION_SOCK_PATH = "/var/run/numaflow/function.sock"
SINK_SOCK_PATH = "/var/run/numaflow/udsink.sock"
# TODO: need to make sure the DATUM_KEY value is the same as
# https://github.com/numaproj/numaflow-go/blob/main/pkg/function/configs.go#L6
DATUM_KEY = "x-numaflow-datum-key"
