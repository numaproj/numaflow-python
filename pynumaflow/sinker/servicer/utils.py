from pynumaflow.proto.sinker import sink_pb2
from pynumaflow.sinker._dtypes import Response


def build_sink_response(rspn: Response):
    if rspn.success:
        return sink_pb2.SinkResponse.Result(id=rspn.id, status=sink_pb2.Status.SUCCESS)
    elif rspn.fallback:
        return sink_pb2.SinkResponse.Result(id=rspn.id, status=sink_pb2.Status.FALLBACK)
    else:
        return sink_pb2.SinkResponse.Result(
            id=rspn.id, status=sink_pb2.Status.FAILURE, err_msg=rspn.err
        )
