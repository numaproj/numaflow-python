import os
import threading
from threading import Thread
import pynumaflow.sideinput as sideinputsdk
from pynumaflow.mapper import Messages, MapServer, Message, Datum
from watchfiles import watch

# Global variable and lock for thread safety
global_variable = "no_value"
global_variable_lock = threading.Lock()

# Side input file that we are watching
watched_file = "myticker"


def my_handler(keys: list[str], datum: Datum) -> Messages:
    with global_variable_lock:
        current_value = global_variable

    messages = Messages()
    messages.append(Message(str.encode(current_value)))
    return messages


def watcher():
    """
    This function is used to watch the side input directory for changes.
    """
    path = sideinputsdk.SIDE_INPUT_DIR_PATH
    for changes in watch(path):
        for change in changes:
            change_type, file_path = change
            if file_path.endswith(watched_file):
                with global_variable_lock:
                    try:
                        with open(file_path, 'r') as file:
                            new_value = file.read().strip()
                            global global_variable
                            global_variable = new_value
                            print(f"Global variable updated to: {global_variable}")
                    except Exception as e:
                        print(f"Error reading file: {e}")


if __name__ == "__main__":
    """
    This function is used to start the GRPC server and the watcher thread.
    """
    # Read the SIDE INPUT FILE for initial value before starting the server
    path = os.path.join(sideinputsdk.SIDE_INPUT_DIR_PATH, watched_file)
    print(path)
    try:
        with open(path, 'r') as file:
            value = file.read().strip()
            global_variable = value
            print(f"Global variable updated to: {global_variable}")
    except Exception as e:
        print(f"Error reading file: {e}")

    daemon = Thread(target=watcher, daemon=True, name="Monitor")
    grpc_server = MapServer(my_handler)
    thread_server = Thread(target=grpc_server.start, daemon=True, name="GRPC Server")
    daemon.start()
    thread_server.start()
    thread_server.join()
    daemon.join()