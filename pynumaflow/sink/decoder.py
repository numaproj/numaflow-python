import base64
import json
from typing import Dict, Any, List, Tuple


def msgpack_decoder(obj_pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    """
    Custom decoder for msgpack. This is needed to convert golang struct fields
    which follow PascalCase to lower case Python fields.

    Args:
        obj_pairs: List of object pairs for the object being unpacked

    Returns: A dictionary containing key value pairs
    """
    result = {}
    for key, value in obj_pairs:
        result[key.lower()] = value
    return result


class NumaflowJSONDecoder(json.JSONDecoder):
    """
    Custom JSON decoder to deal with the payload field of Sink Messages object.
    """

    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_pairs_hook=self.object_pairs_hook, *args, **kwargs)

    @staticmethod
    def object_pairs_hook(elements: tuple) -> Dict[str, Any]:
        obj_pairs = {}
        for key, item in elements:
            if key == "payload":
                obj_pairs[key] = base64.b64decode(item.encode())
            else:
                obj_pairs[key] = item
        return obj_pairs
