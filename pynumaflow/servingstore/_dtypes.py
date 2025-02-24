from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import TypeVar
from collections.abc import Awaitable

P = TypeVar("P", bound="Payload")


@dataclass
class Payload:
    """
    Class to define each independent result stored in the Store for the given ID.

    Attributes:
        origin (str): The origin of a given payload, typically describing where or what the data
                      comes from
        value (bytes): The data associated with the payload, stored as bytes to accommodate various
                       types of binary data or encoded string data.
    """

    origin: str
    value: bytes


@dataclass(init=False)
class PutDatum:
    """
    Class to define data for the Put rpc.
    Args:
        id_: the id of the request.
        payloads: the payload to be stored.

    >>> # Example usage
    >>> from pynumaflow.servingstore import PutDatum
    >>> from datetime import datetime, timezone
    >>> payload = Payload(_id="avc", value=bytes("test_mock_message", encoding="utf-8"))
    >>> d = PutDatum(
    ...      id_ = "avc", payloads = [payload]
    ...    )
    """

    __slots__ = ("_id", "_payloads")

    _id: str
    _payloads: list[Payload]

    def __init__(
        self,
        id_: str,
        payloads: list[Payload],
    ):
        self._id = id_
        self._payloads = payloads or []

    @property
    def id(self) -> str:
        """Returns the id of the event"""
        return self._id

    @property
    def payloads(self) -> list[Payload]:
        """Returns the payloads of the event."""
        return self._payloads


@dataclass(init=False)
class GetDatum:
    """
    Class to retrieve data from the Get rpc.
    Args:
        id_: the id of the request.

    >>> # Example usage
    >>> from pynumaflow.servingstore import GetDatum
    >>> from datetime import datetime, timezone
    >>> payload = bytes("test_mock_message", encoding="utf-8")
    >>> d = GetDatum(
    ...      id_ = "avc"
    ...    )
    """

    __slots__ = ("_id",)

    _id: str

    def __init__(
        self,
        id_: str,
    ):
        self._id = id_

    @property
    def id(self) -> str:
        """Returns the id of the event"""
        return self._id


@dataclass
class StoredResult:
    """
    Class to define the data stored in the store per origin..
    Args:
        id_: unique ID for the response
        payloads: the payloads of the given ID
    """

    __slots__ = ("_id", "_payloads")

    _id: str
    _payloads: list[Payload]

    def __init__(self, id_: str, payloads: list[Payload] = None):
        """
        Creates a StoredResult object to send value to a vertex.
        """
        self._id = id_
        self._payloads = payloads or []

    @property
    def payloads(self) -> list[Payload]:
        """Returns the payloads of the event"""
        return self._payloads

    @property
    def id(self) -> str:
        """Returns the id of the event"""
        return self._id


class ServingStorer(metaclass=ABCMeta):
    """
    Provides an interface to write a Serving Store Class
    which will be exposed over gRPC.
    """

    @abstractmethod
    def put(self, datum: PutDatum):
        """
        This function is called when a Side Input request is received.
        """
        pass

    @abstractmethod
    def get(self, datum: GetDatum) -> [StoredResult, Awaitable[StoredResult]]:
        """
        The simple source always returns zero to indicate there is no pending record.
        """
        pass


ServingStoreCallable = ServingStorer
