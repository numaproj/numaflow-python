import uuid
from pynumaflow.mapper import Datum, Messages, Message

sock_prefix = f"/tmp/test_async_multiproc_map_{uuid.uuid4().hex}_"


async def async_handler(keys, datum: Datum) -> Messages:
    msg = (
        f"payload:{datum.value.decode()} event_time:{datum.event_time} watermark:{datum.watermark}"
    )
    return Messages(Message(value=msg.encode(), keys=keys))


#
# class TestAsyncMapMultiprocServer(unittest.TestCase):
#     def setUp(self):
#         self.base_sock_path = sock_prefix
#         self.server = AsyncMapMultiprocServer(
#             mapper_instance=async_handler,
#             server_count=2,
#             sock_path=self.base_sock_path,
#             use_tcp=False,
#             server_info_file=None,
#         )
#         self.process = Process(target=self.server.start)
#         self.process.start()
#
#         # Wait for both servers to bind
#         self.socket_paths = [f"{self.base_sock_path}{i}.sock" for i in range(2)]
#         for path in self.socket_paths:
#             for _ in range(10):
#                 if os.path.exists(path):
#                     break
#                 time.sleep(0.5)
#
#     def tearDown(self):
#         self.process.terminate()
#         self.process.join()
#         for path in self.socket_paths:
#             try:
#                 os.remove(path)
#             except FileNotFoundError:
#                 pass
#
#     def test_map_fn(self):
#         bind_address = f"unix://{self.socket_paths[0]}"
#         request = get_test_datums()
#         with grpc.insecure_channel(bind_address) as channel:
#             stub = map_pb2_grpc.MapStub(channel)
#             responses_iter = stub.MapFn(request_iterator=request_generator(request))
#             responses = []
#             # capture the output from the ReadFn generator and assert.
#             for r in responses_iter:
#                 responses.append(r)
#
#             # 1 handshake + 3 data responses
#             self.assertEqual(4, len(responses))
#
#             self.assertTrue(responses[0].handshake.sot)
#
#             idx = 1
#             while idx < len(responses):
#                 _id = "test-id-" + str(idx)
#                 self.assertEqual(_id, responses[idx].id)
#                 self.assertEqual(
#                     bytes(
#                         "payload:test_mock_message "
#                         "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
#                         encoding="utf-8",
#                     ),
#                     responses[idx].results[0].value,
#                 )
#                 self.assertEqual(1, len(responses[idx].results))
#                 idx += 1
#
#     def test_server_start(self):
#         for path in self.socket_paths:
#             self.assertTrue(
#                 os.path.exists(path), f"Server socket {path} was not created successfully"
#             )

#
# if __name__ == "__main__":
#     unittest.main()
#
