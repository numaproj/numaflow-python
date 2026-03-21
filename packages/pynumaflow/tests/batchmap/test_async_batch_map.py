import asyncio
import logging
import threading
from collections.abc import AsyncIterable

import grpc
import pytest
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow.batchmapper import (
    Message,
    Datum,
    BatchMapper,
    BatchResponses,
    BatchResponse,
    BatchMapAsyncServer,
)
from pynumaflow.proto.mapper import map_pb2_grpc
from tests.batchmap.utils import request_generator

LOGGER = setup_logging(__name__)

listen_addr = "unix:///tmp/batch_map.sock"


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


class ExampleClass(BatchMapper):
    async def handler(
        self,
        datums: AsyncIterable[Datum],
    ) -> BatchResponses:
        batch_responses = BatchResponses()
        async for datum in datums:
            val = datum.value
            _ = datum.event_time
            _ = datum.watermark
            strs = val.decode("utf-8").split(",")
            batch_response = BatchResponse.from_id(datum.id)
            if len(strs) == 0:
                batch_response.append(Message.to_drop())
            else:
                for s in strs:
                    batch_response.append(Message(str.encode(s)))
            batch_responses.append(batch_response)

        return batch_responses


async def handler(
    datums: AsyncIterable[Datum],
) -> BatchResponses:
    batch_responses = BatchResponses()
    async for datum in datums:
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        strs = val.decode("utf-8").split(",")
        batch_response = BatchResponse.from_id(datum.id)
        if len(strs) == 0:
            batch_response.append(Message.to_drop())
        else:
            for s in strs:
                batch_response.append(Message(str.encode(s)))
        batch_responses.append(batch_response)

    return batch_responses


def NewAsyncBatchMapper():
    d = ExampleClass()
    server_instance = BatchMapAsyncServer(d)
    udfs = server_instance.servicer
    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()


@pytest.fixture(scope="module")
def async_batch_map_server():
    """Module-scoped fixture: starts an async gRPC batch map server in a background thread."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
    thread.start()

    udfs = NewAsyncBatchMapper()
    asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)

    while True:
        try:
            with grpc.insecure_channel(listen_addr) as channel:
                f = grpc.channel_ready_future(channel)
                f.result(timeout=10)
                if f.done():
                    break
        except grpc.FutureTimeoutError as e:
            LOGGER.error("error trying to connect to grpc server")
            LOGGER.error(e)

    yield loop

    loop.stop()
    LOGGER.info("stopped the event loop")


@pytest.fixture()
def batch_map_stub(async_batch_map_server):
    """Returns a MapStub connected to the running async batch map server."""
    return map_pb2_grpc.MapStub(grpc.insecure_channel(listen_addr))


def test_batch_map(batch_map_stub) -> None:
    generator_response = None

    try:
        generator_response = batch_map_stub.MapFn(
            request_iterator=request_generator(count=10, session=1)
        )
    except grpc.RpcError as e:
        logging.error(e)

    handshake = next(generator_response)
    # assert that handshake response is received.
    assert handshake.handshake.sot
    data_resp = []
    for r in generator_response:
        data_resp.append(r)

    idx = 0
    while idx < len(data_resp) - 1:
        assert (
            bytes(
                "test_mock_message",
                encoding="utf-8",
            )
            == data_resp[idx].results[0].value
        )
        _id = data_resp[idx].id
        assert _id == "test-id-" + str(idx)
        idx += 1
    # EOT Response
    assert data_resp[len(data_resp) - 1].status.eot is True
    # 10 sink responses + 1 EOT response
    assert 11 == len(data_resp)


def test_is_ready(async_batch_map_server) -> None:
    with grpc.insecure_channel(listen_addr) as channel:
        stub = map_pb2_grpc.MapStub(channel)

        request = _empty_pb2.Empty()
        response = None
        try:
            response = stub.IsReady(request=request)
        except grpc.RpcError as e:
            logging.error(e)

        assert response.ready


@pytest.mark.parametrize(
    "max_threads_arg,expected",
    [
        (32, 16),  # max cap at 16
        (5, 5),  # use argument provided
        (None, 4),  # defaults to 4
    ],
)
def test_max_threads(max_threads_arg, expected):
    kwargs = {"batch_mapper_instance": handler}
    if max_threads_arg is not None:
        kwargs["max_threads"] = max_threads_arg
    server = BatchMapAsyncServer(**kwargs)
    assert server.max_threads == expected
