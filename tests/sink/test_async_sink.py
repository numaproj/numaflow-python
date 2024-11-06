import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable
from unittest.mock import patch

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow._constants import (
    UD_CONTAINER_FALLBACK_SINK,
    FALLBACK_SINK_SOCK_PATH,
    FALLBACK_SINK_SERVER_INFO_FILE_PATH,
)
from pynumaflow.sinker import (
    Datum,
)
from pynumaflow.sinker import Responses, Response
from pynumaflow.proto.sinker import sink_pb2_grpc, sink_pb2
from pynumaflow.sinker.async_server import SinkAsyncServer
from tests.sink.test_server import (
    mock_message,
    mock_err_message,
    mock_fallback_message,
    mockenv,
)
from tests.testing_utils import get_time_args, mock_terminate_on_stop

LOGGER = setup_logging(__name__)


async def udsink_handler(datums: AsyncIterable[Datum]) -> Responses:
    responses = Responses()
    async for msg in datums:
        if msg.value.decode("utf-8") == "test_mock_err_message":
            raise ValueError("test_mock_err_message")
        elif msg.value.decode("utf-8") == "test_mock_fallback_message":
            responses.append(Response.as_fallback(msg.id))
        else:
            responses.append(Response.as_success(msg.id))
    return responses


def start_sink_streaming_request(_id: str, req_type) -> (Datum, tuple):
    event_time_timestamp, watermark_timestamp = get_time_args()
    value = mock_message()
    if req_type == "err":
        value = mock_err_message()

    if req_type == "fallback":
        value = mock_fallback_message()

    request = sink_pb2.SinkRequest.Request(
        value=value, event_time=event_time_timestamp, watermark=watermark_timestamp, id=_id
    )
    return sink_pb2.SinkRequest(request=request)


def request_generator(count, req_type="success", session=1, handshake=True):
    if handshake:
        yield sink_pb2.SinkRequest(handshake=sink_pb2.Handshake(sot=True))

    for j in range(session):
        for i in range(count):
            yield start_sink_streaming_request(str(i), req_type)

        yield sink_pb2.SinkRequest(status=sink_pb2.TransmissionStatus(eot=True))


_s: Server = None
_channel = grpc.insecure_channel("unix:///tmp/async_sink.sock")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_server():
    server = grpc.aio.server()
    server_instance = SinkAsyncServer(sinker_instance=udsink_handler)
    uds = server_instance.servicer
    sink_pb2_grpc.add_SinkServicer_to_server(uds, server)
    listen_addr = "unix:///tmp/async_sink.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestAsyncSink(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        asyncio.run_coroutine_threadsafe(start_server(), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("unix:///tmp/async_sink.sock") as channel:
                    f = grpc.channel_ready_future(channel)
                    f.result(timeout=10)
                    if f.done():
                        break
            except grpc.FutureTimeoutError as e:
                LOGGER.error("error trying to connect to grpc server")
                LOGGER.error(e)

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            _loop.stop()
            LOGGER.info("stopped the event loop")
        except Exception as e:
            LOGGER.error(e)

    #
    def test_run_server(self) -> None:
        with grpc.insecure_channel("unix:///tmp/async_sink.sock") as channel:
            stub = sink_pb2_grpc.SinkStub(channel)

            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.IsReady(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertTrue(response.ready)

    def test_sink(self) -> None:
        stub = self.__stub()
        generator_response = None
        grpc_exception = None
        try:
            generator_response = stub.SinkFn(
                request_iterator=request_generator(count=10, req_type="success", session=1)
            )
            handshake = next(generator_response)
            # assert that handshake response is received.
            self.assertTrue(handshake.handshake.sot)

            data_resp = []
            for r in generator_response:
                data_resp.append(r)

            # 1 sink data response + 1 EOT response
            self.assertEqual(2, len(data_resp))

            idx = 0
            # capture the output from the SinkFn generator and assert.
            for resp in data_resp[0].results:
                self.assertEqual(resp.id, str(idx))
                self.assertEqual(resp.status, sink_pb2.Status.SUCCESS)
                idx += 1
            # EOT Response
            self.assertEqual(data_resp[1].status.eot, True)

        except grpc.RpcError as e:
            logging.error(e)
            grpc_exception = e

        self.assertIsNone(grpc_exception)

    def test_sink_err(self) -> None:
        stub = self.__stub()
        grpc_exception = None
        try:
            generator_response = stub.SinkFn(
                request_iterator=request_generator(count=10, req_type="err")
            )
            for _ in generator_response:
                pass
        except BaseException as e:
            self.assertTrue("UDSinkError: ValueError('test_mock_err_message')" in e.__str__())
            return
        except grpc.RpcError as e:
            grpc_exception = e
            self.assertEqual(grpc.StatusCode.UNKNOWN, e.code())
            print(e.details())

        self.assertIsNotNone(grpc_exception)

    def test_sink_err_handshake(self) -> None:
        stub = self.__stub()
        grpc_exception = None
        try:
            generator_response = stub.SinkFn(
                request_iterator=request_generator(count=10, req_type="success", handshake=False)
            )
            for _ in generator_response:
                pass
        except BaseException as e:
            self.assertTrue("ReadFn: expected handshake message" in e.__str__())
            return
        except grpc.RpcError as e:
            grpc_exception = e
            self.assertEqual(grpc.StatusCode.UNKNOWN, e.code())
            print(e.details())

        self.assertIsNotNone(grpc_exception)

    def test_sink_fallback(self) -> None:
        stub = self.__stub()
        try:
            generator_response = stub.SinkFn(
                request_iterator=request_generator(count=10, req_type="fallback", session=1)
            )
            handshake = next(generator_response)
            # assert that handshake response is received.
            self.assertTrue(handshake.handshake.sot)

            data_resp = []
            for r in generator_response:
                data_resp.append(r)

            # 1 sink data response + 1 EOT response
            self.assertEqual(2, len(data_resp))

            idx = 0
            # capture the output from the SinkFn generator and assert.
            for resp in data_resp[0].results:
                self.assertEqual(resp.id, str(idx))
                self.assertEqual(resp.status, sink_pb2.Status.FALLBACK)
                idx += 1
            # EOT Response
            self.assertEqual(data_resp[1].status.eot, True)

        except grpc.RpcError as e:
            logging.error(e)

    def __stub(self):
        return sink_pb2_grpc.SinkStub(_channel)

    def test_invalid_server_type(self) -> None:
        with self.assertRaises(TypeError):
            SinkAsyncServer()

    @mockenv(NUMAFLOW_UD_CONTAINER_TYPE=UD_CONTAINER_FALLBACK_SINK)
    def test_start_fallback_sink(self):
        server = SinkAsyncServer(sinker_instance=udsink_handler)
        self.assertEqual(server.sock_path, f"unix://{FALLBACK_SINK_SOCK_PATH}")
        self.assertEqual(server.server_info_file, FALLBACK_SINK_SERVER_INFO_FILE_PATH)

    def test_max_threads(self):
        # max cap at 16
        server = SinkAsyncServer(sinker_instance=udsink_handler, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = SinkAsyncServer(sinker_instance=udsink_handler, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = SinkAsyncServer(sinker_instance=udsink_handler)
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
