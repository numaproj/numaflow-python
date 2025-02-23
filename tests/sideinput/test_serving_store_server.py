import unittest
from unittest.mock import patch

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.proto.serving import store_pb2
from pynumaflow.servingstore import (
    ServingStorer,
    PutDatum,
    Payload,
    GetDatum,
    StoredResult,
    ServingStoreServer,
)
from tests.testing_utils import mock_terminate_on_stop


class InMemoryStore(ServingStorer):
    def __init__(self):
        self.store = {}

    def put(self, datum: PutDatum):
        req_id = datum.id
        print("Received Put request for ", req_id)
        if req_id not in self.store:
            self.store[req_id] = []

        cur_payloads = self.store[req_id]
        for x in datum.payloads:
            print(x)
            cur_payloads.append(Payload(x.origin, x.value))
        self.store[req_id] = cur_payloads

    def get(self, datum: GetDatum) -> StoredResult:
        req_id = datum.id
        print("Received Get request for ", req_id)
        resp = []
        if req_id in self.store:
            resp = self.store[req_id]
        return StoredResult(id_=req_id, payloads=resp)


class ErrInMemoryStore(ServingStorer):
    def __init__(self):
        self.store = {}

    def put(self, datum: PutDatum):
        req_id = datum.id
        print("Received Put request for ", req_id)
        if req_id not in self.store:
            self.store[req_id] = []

        cur_payloads = self.store[req_id]
        for x in datum.payloads:
            cur_payloads.append(Payload(x.origin, x.value))
        raise ValueError("something fishy")
        self.store[req_id] = cur_payloads

    def get(self, datum: GetDatum) -> StoredResult:
        req_id = datum.id
        print("Received Get request for ", req_id)
        raise ValueError("get is fishy")


def mock_message():
    msg = bytes("test_side_input", encoding="utf-8")
    return msg


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestServer(unittest.TestCase):
    """
    Test the SideInput grpc server
    """

    def setUp(self) -> None:
        self.InMem = InMemoryStore()
        server = ServingStoreServer(self.InMem)
        my_service = server.servicer
        services = {store_pb2.DESCRIPTOR.services_by_name["ServingStore"]: my_service}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_init_with_args(self) -> None:
        """
        Test the initialization of the SideInput class,
        """
        my_server = ServingStoreServer(
            serving_store_instance=InMemoryStore(),
            sock_path="/tmp/test_serving_store.sock",
            max_message_size=1024 * 1024 * 5,
        )
        self.assertEqual(my_server.sock_path, "unix:///tmp/test_serving_store.sock")
        self.assertEqual(my_server.max_message_size, 1024 * 1024 * 5)

    def test_serving_store_err(self):
        """
        Test the error case for the Put method,
        """
        server = ServingStoreServer(ErrInMemoryStore())
        my_service = server.servicer
        services = {store_pb2.DESCRIPTOR.services_by_name["ServingStore"]: my_service}
        self.test_server = server_from_dictionary(services, strict_real_time())

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                store_pb2.DESCRIPTOR.services_by_name["ServingStore"].methods_by_name["Put"]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=store_pb2.PutRequest(
                id="abc",
                payloads=[
                    store_pb2.Payload(origin="abc", value=bytes("test_put", encoding="utf-8"))
                ],
            ),
            timeout=1,
        )
        response, metadata, code, details = method.termination()
        self.assertEqual(grpc.StatusCode.UNKNOWN, code)
        self.assertTrue("something fishy" in details)

    def test_is_ready(self):
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                store_pb2.DESCRIPTOR.services_by_name["ServingStore"].methods_by_name["IsReady"]
            ),
            invocation_metadata={},
            request=_empty_pb2.Empty(),
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        expected = store_pb2.ReadyResponse(ready=True)
        self.assertEqual(expected, response)
        self.assertEqual(code, StatusCode.OK)

    def test_put_message(self):
        """
        Test the broadcast_message method,
        where we expect the no_broadcast flag to be False and
        the message value to be the mock_message.
        """
        request = store_pb2.PutRequest(
            id="abc",
            payloads=[store_pb2.Payload(origin="abc1", value=bytes("test_put", encoding="utf-8"))],
        )
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                store_pb2.DESCRIPTOR.services_by_name["ServingStore"].methods_by_name["Put"]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=request,
            timeout=1,
        )
        response, metadata, code, details = method.termination()
        self.assertEqual(True, response.success)
        self.assertEqual(code, StatusCode.OK)
        stored = self.InMem.store["abc"]
        self.assertEqual(stored[0].origin, "abc1")
        self.assertEqual(stored[0].value, bytes("test_put", encoding="utf-8"))

    def test_get_message(self):
        """
        Test the broadcast_message method,
        where we expect the no_broadcast flag to be False and
        the message value to be the mock_message.
        """
        request = store_pb2.GetRequest(
            id="abc",
        )

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                store_pb2.DESCRIPTOR.services_by_name["ServingStore"].methods_by_name["Get"]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=request,
            timeout=1,
        )
        pl = Payload(origin="abc", value=bytes("test_put", encoding="utf-8"))
        self.InMem.store["abc"] = [pl]
        response, metadata, code, details = method.termination()
        self.assertEqual(len(response.payloads), 1)
        self.assertEqual(code, StatusCode.OK)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            ServingStoreServer()

    def test_max_threads(self):
        # max cap at 16
        server = ServingStoreServer(InMemoryStore(), max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = ServingStoreServer(InMemoryStore(), max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = ServingStoreServer(InMemoryStore())
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    unittest.main()
