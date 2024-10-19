import os
import threading
from threading import Thread
import pynumaflow.sideinput as sideinputsdk
from pynumaflow.mapper import Messages, MapServer, Message, Datum, Mapper
from watchfiles import watch


class SideInputHandler(Mapper):
    """
    This is a class that inherits from the Mapper class.
    It implements the handler method that is called for each datum.
    """
    # variable and lock for thread safety
    data_value = "no_value"
    data_value_lock = threading.Lock()

    # Side input file that we are watching
    watched_file = "myticker"

    def handler(self, keys: list[str], datum: Datum) -> Messages:
        with self.data_value_lock:
            current_value = self.data_value

        messages = Messages()
        messages.append(Message(str.encode(current_value)))
        return messages

    def file_watcher(self):
        """
        This function is used to watch the side input directory for changes.
        """
        path = sideinputsdk.SIDE_INPUT_DIR_PATH
        for changes in watch(path):
            for change in changes:
                change_type, file_path = change
                if file_path.endswith(self.watched_file):
                    with self.data_value_lock:
                        self.update_data_from_file(file_path)

    def init_data_value(self):
        # Read the SIDE INPUT FILE for initial value before starting the server
        path = os.path.join(sideinputsdk.SIDE_INPUT_DIR_PATH, self.watched_file)
        print(path)
        self.update_data_from_file(path)

    def update_data_from_file(self, path):
        try:
            with open(path) as file:
                value = file.read().strip()
                self.data_value = value
                print(f"Data value variable set to: {self.data_value}")
        except Exception as e:
            print(f"Error reading file: {e}")


if __name__ == "__main__":
    """
    This function is used to start the GRPC server and the file_watcher thread.
    """
    handler_instance = SideInputHandler()

    # initialize data with value from side input
    handler_instance.init_data_value()

    daemon = Thread(target=handler_instance.file_watcher, daemon=True, name="Monitor")
    grpc_server = MapServer(handler_instance)
    thread_server = Thread(target=grpc_server.start, daemon=True, name="GRPC Server")
    daemon.start()
    thread_server.start()
    thread_server.join()
    daemon.join()
