import json
from dataclasses import dataclass
from typing import List, TypeVar, Type, Optional

import msgpack

from pynumaflow._constants import APPLICATION_JSON, APPLICATION_MSG_PACK
from pynumaflow.encoder import NumaflowJSONEncoder, msgpack_encoding
from pynumaflow.exceptions import MarshalError

R = TypeVar("R", bound="Response")
Rs = TypeVar("Rs", bound="Responses")


@dataclass
class Message:
    id: str
    payload: bytes


@dataclass
class Response:
    id: str
    success: bool
    err: Optional[str]

    @classmethod
    def as_success(cls: Type[R], id_: str) -> R:
        return Response(id=id_, success=True, err=None)

    @classmethod
    def as_failure(cls: Type[R], id_: str, err_msg: str) -> R:
        return Response(id=id_, success=False, err=err_msg)


class Responses:
    def __init__(self, *responses: R):
        self._responses = list(responses) or []

    def __str__(self):
        return str(self._responses)

    def __repr__(self):
        return str(self)

    def append(self, response: R) -> None:
        return self._responses.append(response)

    def items(self) -> List[R]:
        return self._responses

    def dumps(self, udf_content_type: str) -> str:
        if udf_content_type == APPLICATION_JSON:
            return json.dumps(self._responses, cls=NumaflowJSONEncoder, separators=(",", ":"))
        elif udf_content_type == APPLICATION_MSG_PACK:
            return msgpack.dumps(self._responses, default=msgpack_encoding)
        raise MarshalError(udf_content_type)
