"""
Side Input Example for pynumaflow-lite.

This module contains both a SideInput retriever and a Mapper that reads from side inputs.
The mode is controlled by the MAPPER environment variable:
- If MAPPER is set to "true", runs as a Mapper that reads side input files
- Otherwise, runs as a SideInput retriever that broadcasts values
"""
import asyncio
import os
import signal
import threading
from threading import Thread
import datetime

from pynumaflow_lite import sideinputer, mapper
from watchfiles import watch


class ExampleSideInput(sideinputer.SideInput):
    """
    A SideInput retriever that broadcasts a timestamp message every time.
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
        # broadcast_message() is used to indicate that there is a broadcast
        return sideinputer.Response.broadcast_message(val.encode("utf-8"))


class SideInputHandler(mapper.Mapper):
    """
    A Mapper that reads from side input files and includes the value in its output.
    """

    # variable and lock for thread safety
    data_value = "no_value"
    data_value_lock = threading.Lock()

    # Side input file that we are watching
    watched_file = "myticker"

    async def handler(self, keys: list[str], datum: mapper.Datum) -> mapper.Messages:
        with self.data_value_lock:
            current_value = self.data_value

        messages = mapper.Messages()
        messages.append(mapper.Message(str.encode(current_value)))
        return messages

    def file_watcher(self):
        """
        This function is used to watch the side input directory for changes.
        """
        path = sideinputer.DIR_PATH
        for changes in watch(path):
            for change in changes:
                change_type, file_path = change
                if file_path.endswith(self.watched_file):
                    with self.data_value_lock:
                        self.update_data_from_file(file_path)

    def init_data_value(self):
        """Read the SIDE INPUT FILE for initial value before starting the server."""
        path = os.path.join(sideinputer.DIR_PATH, self.watched_file)
        print(f"Initializing side input from: {path}")
        self.update_data_from_file(path)

    def update_data_from_file(self, path):
        try:
            with open(path) as file:
                value = file.read().strip()
                self.data_value = value
                print(f"Data value variable set to: {self.data_value}")
        except Exception as e:
            print(f"Error reading file: {e}")


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass


async def start_sideinput():
    """Start the SideInput retriever server."""
    server = sideinputer.SideInputAsyncServer()
    side_input = ExampleSideInput()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
    loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())

    try:
        await server.start(side_input)
        print("SideInput server shutting down gracefully...")
    except asyncio.CancelledError:
        server.stop()


async def start_mapper():
    """Start the Mapper server that reads from side inputs."""
    server = mapper.MapAsyncServer()
    handler = SideInputHandler()

    # Initialize the data value from the side input file
    handler.init_data_value()

    # Start the file watcher in a background thread
    watcher_thread = Thread(target=handler.file_watcher, daemon=True)
    watcher_thread.start()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
    loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())

    try:
        await server.start(handler)
        print("Mapper server shutting down gracefully...")
    except asyncio.CancelledError:
        server.stop()


if __name__ == "__main__":
    # Check if we should run as a mapper or side input retriever
    is_mapper = os.environ.get("MAPPER", "").lower() == "true"

    if is_mapper:
        print("Starting as Mapper (reading side inputs)...")
        asyncio.run(start_mapper())
    else:
        print("Starting as SideInput retriever...")
        asyncio.run(start_sideinput())

