import base64
import json
from typing import Dict, Any


def msgpack_encoding(obj) -> Dict[str, Any]:
    """
    Custom callable for msgpack to deal with types
    involving User Defined Sinks.

    Args:
        obj: Object to encode

    Returns:
        Dictionary representation of the object
    """
    from pynumaflow.sink import Response, Message as UDSinkMessage

    if isinstance(obj, UDSinkMessage):
        obj = {"ID": obj.id, "Payload": obj.payload}
    if isinstance(obj, Response):
        return {"ID": obj.id, "Success": obj.success, "Err": obj.err}
    return obj


class NumaflowJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to deal with types involving User Defined Sinks.
    """

    def default(self, obj):
        from pynumaflow.sink import Response, Message as UDSinkMessage

        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode("utf-8")
        if isinstance(obj, Response):
            return {"id": obj.id, "success": obj.success, "err": obj.err}
        if isinstance(obj, UDSinkMessage):
            return {"id": obj.id, "payload": obj.payload}
        return json.JSONEncoder.default(self, obj)
