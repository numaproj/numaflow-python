import datetime
from pynumaflow.sideinput import Response, SideInputServer, SideInput


class ExampleSideInput(SideInput):
    def __init__(self):
        self.counter = 0

    def retrieve_handler(self) -> Response:
        """
        This function is called every time the side input is requested.
        """
        time_now = datetime.datetime.now()
        # val is the value to be broadcasted
        val = "an example:" + str(time_now)
        self.counter += 1
        # broadcast every other time
        if self.counter % 2 == 0:
            # no_broadcast_message() is used to indicate that there is no broadcast
            return Response.no_broadcast_message()
        # broadcast_message() is used to indicate that there is a broadcast
        return Response.broadcast_message(val.encode("utf-8"))


if __name__ == "__main__":
    grpc_server = SideInputServer(ExampleSideInput())
    grpc_server.start()
