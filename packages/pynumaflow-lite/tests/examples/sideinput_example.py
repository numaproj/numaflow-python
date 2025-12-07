import asyncio
import signal
import datetime
from pynumaflow_lite import sideinputer


class ExampleSideInput(sideinputer.SideInput):
    """
    An example SideInput that broadcasts a message every other time.
    """

    def __init__(self):
        self.counter = 0

    async def retrieve_handler(self) -> sideinputer.Response:
        """
        This function is called every time the side input is requested.
        """
        time_now = datetime.datetime.now()
        # val is the value to be broadcasted
        val = f"an example: {str(time_now)}"
        self.counter += 1
        # broadcast every other time
        if self.counter % 2 == 0:
            # no_broadcast_message() is used to indicate that there is no broadcast
            return sideinputer.Response.no_broadcast_message()
        # broadcast_message() is used to indicate that there is a broadcast
        return sideinputer.Response.broadcast_message(val.encode("utf-8"))


async def main():
    # Create the server with custom socket paths for testing
    server = sideinputer.SideInputAsyncServer(
        sock_file="/tmp/var/run/numaflow/sideinput.sock",
        info_file="/tmp/var/run/numaflow/sideinput-server-info",
    )

    # Create the side input instance
    side_input = ExampleSideInput()

    # Set up signal handling for graceful shutdown
    loop = asyncio.get_running_loop()
    
    def handle_signal():
        server.stop()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    # Start the server
    await server.start(side_input)


if __name__ == "__main__":
    asyncio.run(main())

