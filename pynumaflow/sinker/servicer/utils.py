from pynumaflow.proto.sinker import sink_pb2
from pynumaflow.sinker._dtypes import Response


def build_sink_response(rspn: Response):
    if rspn.fallback:
        return sink_pb2.SinkResponse.Result(id=rspn.id, status=sink_pb2.Status.FALLBACK)
    elif rspn.success:
        return sink_pb2.SinkResponse.Result(id=rspn.id, status=sink_pb2.Status.SUCCESS)
    else:
        return sink_pb2.SinkResponse.Result(
            id=rspn.id, status=sink_pb2.Status.FAILURE, err_msg=rspn.err
        )
