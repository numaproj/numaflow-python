from typing import Optional

from pynumaflow._metadata import _user_and_system_metadata_from_proto
from pynumaflow.proto.sinker import sink_pb2
from pynumaflow.sinker._dtypes import Response, Datum, Responses, Message


def build_sink_resp_results(responses: Responses) -> list[sink_pb2.SinkResponse.Result]:
    """
    Given a list of Response objects, build the corresponding list of SinkResponse.Result objects.

    Parameters:
    responses (Responses): A list of Response objects containing the results of sink operations.

    Returns:
    list[sink_pb2.SinkResponse.Result]: A list of SinkResponse.Result objects.
    """
    return [build_sink_response(rspn) for rspn in responses]


def build_sink_response(rspn: Response) -> sink_pb2.SinkResponse.Result:
    """
    Build a SinkResponse.Result object from a Response object.

    Parameters:
    rspn (Response): A Response object containing the result information of a single sink operation.

    Returns:
    sink_pb2.SinkResponse.Result: A SinkResponse.Result
     object populated with the status and id of the response.
    """
    rid = rspn.id
    if rspn.success:
        return sink_pb2.SinkResponse.Result(id=rid, status=sink_pb2.Status.SUCCESS)
    elif rspn.fallback:
        return sink_pb2.SinkResponse.Result(id=rid, status=sink_pb2.Status.FALLBACK)
    elif rspn.on_success:
        return sink_pb2.SinkResponse.Result(
            id=rid,
            status=sink_pb2.Status.ON_SUCCESS,
            on_success_msg=build_on_success_message(rspn.on_success_msg),
        )
    else:
        return sink_pb2.SinkResponse.Result(
            id=rid, status=sink_pb2.Status.FAILURE, err_msg=rspn.err
        )


def build_on_success_message(
    msg: Optional[Message],
) -> Optional[sink_pb2.SinkResponse.Result.Message]:
    if msg is None:
        return None

    metadata = msg.user_metadata._to_proto() if msg.user_metadata is not None else None

    return sink_pb2.SinkResponse.Result.Message(
        keys=msg.keys,
        value=msg.value,
        metadata=metadata,
    )


def datum_from_sink_req(d: sink_pb2.SinkRequest) -> Datum:
    """
    Convert a SinkRequest object to a Datum object.

    Parameters:
    d (sink_pb2.SinkRequest): A SinkRequest object containing the input data.

    Returns:
    Datum: A Datum object populated with the data from the input SinkRequest object.
    """
    user_metadata, system_metadata = _user_and_system_metadata_from_proto(d.request.metadata)
    datum = Datum(
        keys=list(d.request.keys),
        sink_msg_id=d.request.id,
        value=d.request.value,
        event_time=d.request.event_time.ToDatetime(),
        watermark=d.request.watermark.ToDatetime(),
        headers=dict(d.request.headers),
        user_metadata=user_metadata,
        system_metadata=system_metadata,
    )
    return datum


def _create_read_handshake_response() -> sink_pb2.SinkResponse:
    """
    Create a handshake response for the Sink function.

    Returns:
    sink_pb2.SinkResponse: A SinkResponse object indicating a successful handshake.
    """
    return sink_pb2.SinkResponse(
        handshake=sink_pb2.Handshake(sot=True),
    )
