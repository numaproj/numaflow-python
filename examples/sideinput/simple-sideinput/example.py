import datetime
from pynumaflow.sideinput import Response, SideInputServer

counter = 0


def my_handler() -> Response:
    """
    This function is called every time the side input is requested.
    """
    time_now = datetime.datetime.now()
    # val is the value to be broadcasted
    val = "an example:" + str(time_now)
    global counter
    counter += 1
    # broadcast every other time
    if counter % 2 == 0:
        # no_broadcast_message() is used to indicate that there is no broadcast
        return Response.no_broadcast_message()
    # broadcast_message() is used to indicate that there is a broadcast
    return Response.broadcast_message(val.encode("utf-8"))


if __name__ == "__main__":
    grpc_server = SideInputServer(side_input_instance=my_handler)
    grpc_server.start()
