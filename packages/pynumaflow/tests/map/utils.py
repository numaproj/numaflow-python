from pynumaflow.proto.common import metadata_pb2
from pynumaflow.proto.mapper import map_pb2

from pynumaflow.mapper import Datum, Messages, Message, Mapper
from tests.testing_utils import mock_message, mock_headers, get_time_args


async def async_map_error_fn(keys: list[str], datum: Datum) -> Messages:
    raise ValueError("error invoking map")


class ExampleMap(Mapper):
    def handler(self, keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        msg = "payload:{} event_time:{} watermark:{}".format(
            val.decode("utf-8"),
            datum.event_time,
            datum.watermark,
        )
        val = bytes(msg, encoding="utf-8")
        messages = Messages()
        messages.append(Message(val, keys=keys))
        return messages


def map_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    msg = "payload:{} event_time:{} watermark:{}".format(
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )
    val = bytes(msg, encoding="utf-8")
    messages = Messages()
    messages.append(Message(val, keys=keys))
    return messages


async def async_err_map_handler(_: list[str], __: Datum) -> Messages:
    raise RuntimeError("Something is fishy!")


def err_map_handler(_: list[str], __: Datum) -> Messages:
    raise RuntimeError("Something is fishy!")


def get_test_datums(handshake=True):
    event_time_timestamp, watermark_timestamp = get_time_args()

    responses = []

    if handshake:
        responses.append(
            map_pb2.MapRequest(
                handshake=map_pb2.Handshake(sot=True),
            )
        )

    test_datum = [
        map_pb2.MapRequest(
            request=map_pb2.MapRequest.Request(
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                headers=mock_headers(),
                metadata=metadata_pb2.Metadata(
                    previous_vertex="test-source",
                    sys_metadata={
                        "numaflow_version_info": metadata_pb2.KeyValueGroup(
                            key_value={
                                "version": b"1.0.0",
                                "build_time": b"2021-01-01 00:00:00",
                                "commit_hash": b"1234567890",
                            }
                        ),
                    },
                    user_metadata={
                        "custom_info": metadata_pb2.KeyValueGroup(
                            key_value={
                                "version": b"1.0.0",
                            }
                        ),
                        "test_group": metadata_pb2.KeyValueGroup(
                            key_value={
                                "key1": b"value1",
                                "key2": b"value2",
                            }
                        )
                    },
                ),
            ),
            id="test-id-1",
        ),
        map_pb2.MapRequest(
            request=map_pb2.MapRequest.Request(
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                headers=mock_headers(),
                metadata=metadata_pb2.Metadata(
                    previous_vertex="test-source",
                    sys_metadata={
                        "numaflow_version_info": metadata_pb2.KeyValueGroup(
                            key_value={
                                "version": b"1.0.0",
                                "build_time": b"2021-01-01 00:00:00",
                                "commit_hash": b"1234567890",
                            }
                        ),
                    },
                    user_metadata={
                        "custom_info": metadata_pb2.KeyValueGroup(
                            key_value={
                                "version": b"2.0.0",
                            }
                        ),
                        "test_group": metadata_pb2.KeyValueGroup(
                            key_value={
                                "key1": b"value1",
                                "key2": b"value2",
                            }
                        )
                    },
                ),
            ),
            id="test-id-2",
        ),
        map_pb2.MapRequest(
            request=map_pb2.MapRequest.Request(
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                headers=mock_headers(),
                metadata=metadata_pb2.Metadata(
                    previous_vertex="test-source",
                    sys_metadata={
                        "numaflow_version_info": metadata_pb2.KeyValueGroup(
                            key_value={
                                "version": b"1.0.0",
                                "build_time": b"2021-01-01 00:00:00",
                                "commit_hash": b"1234567890",
                            }
                        ),
                    },
                    user_metadata={
                        "custom_info": metadata_pb2.KeyValueGroup(
                            key_value={
                                "version": b"3.0.0",
                            }
                        ),
                        "test_group": metadata_pb2.KeyValueGroup(
                            key_value={
                                "key1": b"value1",
                                "key2": b"value2",
                            }
                        )
                    },
                ),
            ),
            id="test-id-3",
        ),
    ]
    for x in test_datum:
        responses.append(x)
    return responses
