import os
import warnings
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import TypeAlias

from pynumaflow._metadata import UserMetadata
from pynumaflow._validate import _validate_message_fields
from pynumaflow.shared.asynciter import NonBlockingIterator


@dataclass(init=False, slots=True)
class Offset:
    """
    Args:
        offset: the offset of the datum.
        partition_id: partition_id indicates which partition of the source the datum belongs to.
    """

    _offset: bytes
    _partition_id: int

    def __init__(self, offset: bytes, partition_id: int):
        self._offset = offset
        self._partition_id = partition_id

    @classmethod
    def offset_with_default_partition_id(cls, offset: bytes):
        """
        Returns an Offset object with the given offset and default partition id.
        """
        return Offset(offset=offset, partition_id=get_default_partitions()[0])

    @property
    def as_dict(self):
        return {"offset": self._offset, "partition_id": self._partition_id}

    @property
    def offset(self) -> bytes:
        return self._offset

    @property
    def partition_id(self) -> int:
        return self._partition_id


@dataclass(init=False, slots=True)
class Message:
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        payload: data in bytes
        offset: the offset of the datum.
        event_time: event time of the message, usually extracted from the payload.
        keys: list of string keys for the vertex (optional)
        headers: dict of headers for the message (optional)
        user_metadata: metadata for the message (optional)
    """

    _payload: bytes
    _offset: Offset
    _event_time: datetime
    _keys: list[str]
    _headers: dict[str, str]
    _user_metadata: UserMetadata

    def __init__(
        self,
        payload: bytes,
        offset: Offset,
        event_time: datetime,
        keys: list[str] | None = None,
        headers: dict[str, str] | None = None,
        user_metadata: UserMetadata | None = None,
    ):
        """
        Creates a Message object to send value to a vertex.
        """
        _validate_message_fields(payload, keys, None)
        self._payload = payload
        self._offset = offset
        self._event_time = event_time
        self._keys = keys or []
        self._headers = headers or {}
        self._user_metadata = user_metadata or UserMetadata()

    @property
    def payload(self) -> bytes:
        return self._payload

    @property
    def keys(self) -> list[str]:
        return self._keys

    @property
    def offset(self) -> Offset:
        return self._offset

    @property
    def event_time(self) -> datetime:
        return self._event_time

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    @property
    def user_metadata(self) -> UserMetadata:
        """Returns the user metadata of the message."""
        return self._user_metadata


@dataclass(init=False, slots=True)
class ReadRequest:
    """
    Class to define the request for reading datum stream from user defined source.

    Args:
        num_records: the number of records to read.
        timeout_in_ms: the request timeout in milliseconds.

    Example:
    ```py
    from pynumaflow.sourcer import ReadRequest
    read_request = ReadRequest(num_records=10, timeout_in_ms=1000)
    ```
    """

    _num_records: int
    _timeout_in_ms: int

    def __init__(
        self,
        num_records: int,
        timeout_in_ms: int,
    ):
        if not isinstance(num_records, int):
            raise TypeError(f"Wrong data type: {type(num_records)} for ReadRequest.num_records")
        self._num_records = num_records
        if not isinstance(timeout_in_ms, int):
            raise TypeError(f"Wrong data type: {type(timeout_in_ms)} for ReadRequest.timeout_in_ms")
        self._timeout_in_ms = timeout_in_ms

    @property
    def num_records(self) -> int:
        """Returns the num_records of the request"""
        return self._num_records

    @property
    def timeout_in_ms(self) -> int:
        """Returns the timeout_in_ms of the request."""
        return self._timeout_in_ms


@dataclass(init=False, slots=True)
class AckRequest:
    """
    Class for defining the request for acknowledging datum.
    It takes a list of offsets that need to be acknowledged.

    Args:
        offsets: the offsets to be acknowledged.

    Example:
    ```py
    from pynumaflow.sourcer import AckRequest, Offset

    offset_val = Offset(offset=b"123", partition_id=0)
    ack_request = AckRequest(offsets=[offset_val, offset_val])
    ```
    """

    _offsets: list[Offset]

    def __init__(self, offsets: list[Offset]):
        self._offsets = offsets

    @property
    def offsets(self) -> list[Offset]:
        """Returns the offsets to be acknowledged."""
        return self._offsets


@dataclass
class NackRequest:
    """
    Class for defining the request for negatively acknowledging an offset.
    It takes a list of offsets that need to be negatively acknowledged on the source.

    Args:
        offsets: the offsets to be negatively acknowledged.

    Example:
    ```py
    from pynumaflow.sourcer import NackRequest, Offset
    offset_val = Offset(offset=b"123", partition_id=0)
    nack_request = NackRequest(offsets=[offset_val, offset_val])
    ```
    """

    __slots__ = ("_offsets",)
    _offsets: list[Offset]

    def __init__(self, offsets: list[Offset]):
        self._offsets = offsets

    @property
    def offsets(self) -> list[Offset]:
        """Returns the offsets to be negatively acknowledged."""
        return self._offsets


@dataclass(init=False, slots=True)
class PendingResponse:
    """
    PendingResponse is the response for the pending request.
    It indicates the number of pending records at the user defined source.
    A negative count indicates that the pending information is not available.

    Args:
        count: the number of pending records.
    """

    _count: int

    def __init__(self, count: int):
        if not isinstance(count, int):
            raise TypeError(f"Wrong data type: {type(count)} for Pending.count")
        self._count = count

    @property
    def count(self) -> int:
        """Returns the count of pending records"""
        return self._count


@dataclass(init=False, slots=True)
class PartitionsResponse:
    """
    PartitionsResponse is the response for the partition request.
    It indicates the active partitions at the user defined source.

    Args:
        partitions: the list of active partitions.
        total_partitions: the total number of partitions in the source (optional).
    """

    _partitions: list[int]
    _total_partitions: int | None

    def __init__(self, partitions: list[int], total_partitions: int | None = None):
        if not isinstance(partitions, list):
            raise TypeError(f"Wrong data type: {type(partitions)} for Partition.partitions")
        self._partitions = partitions
        self._total_partitions = total_partitions

    @property
    def partitions(self) -> list[int]:
        """Returns the list of active partitions"""
        return self._partitions

    @property
    def total_partitions(self) -> int | None:
        """Returns the total number of partitions, or None if not available"""
        return self._total_partitions


class Sourcer(metaclass=ABCMeta):
    """
    Provides an interface to write a Sourcer
    which will be exposed over an gRPC server.
    """

    @abstractmethod
    async def read_handler(self, datum: ReadRequest, output: NonBlockingIterator):
        """
        Implement this handler function which implements the SourceReadCallable interface.
        read_handler is used to read the data from the source and send the data forward
        for each read request we process num_records and increment the read_idx to indicate that
        the message has been read and the same is added to the ack set
        """
        pass

    @abstractmethod
    async def ack_handler(self, ack_request: AckRequest):
        """
        The ack handler is used to acknowledge the offsets that have been read
        """
        pass

    @abstractmethod
    async def nack_handler(self, nack_request: NackRequest):
        """
        The nack handler is used to negatively acknowledge the offsets on the source
        """
        pass

    @abstractmethod
    async def pending_handler(self) -> PendingResponse:
        """
        The simple source always returns zero to indicate there is no pending record.
        """
        pass

    async def partitions_handler(self) -> PartitionsResponse:
        """
        .. deprecated::
            Use :meth:`active_partitions_handler` instead.

        Returns the active partitions associated with the source.
        """
        warnings.warn(
            "partitions_handler is deprecated, use active_partitions_handler instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return PartitionsResponse(partitions=get_default_partitions())

    async def active_partitions_handler(self) -> PartitionsResponse:
        """
        Returns the active partitions associated with the source, used by the platform
        to determine the partitions to which the watermark should be published.
        Falls back to partitions_handler() if not overridden.
        """
        return await self.partitions_handler()

    async def total_partitions_handler(self) -> int | None:
        """
        Returns the total number of partitions in the source.
        Used by the platform for watermark progression to know when all
        processors have reported in.
        Returns None by default, indicating the source does not report total partitions.
        """
        return None


# Create default partition id from the environment variable "NUMAFLOW_REPLICA"
DefaultPartitionId = int(os.getenv("NUMAFLOW_REPLICA", "0"))
SourceReadCallable: TypeAlias = Callable[[ReadRequest], Iterator[Message]]
AsyncSourceReadCallable: TypeAlias = Callable[[ReadRequest], AsyncIterator[Message]]
SourceAckCallable: TypeAlias = Callable[[AckRequest], None]
SourceCallable: TypeAlias = Sourcer


def get_default_partitions() -> list[int]:
    """
    Returns the default partition ids.
    """
    return [DefaultPartitionId]
