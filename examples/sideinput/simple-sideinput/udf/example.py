from threading import Thread
import pynumaflow.sideinput as sideinputsdk
from pynumaflow.mapper import Messages, Mapper, Message, Datum
from watchfiles import watch


def my_handler(keys: list[str], datum: Datum) -> Messages:
    messages = Messages()
    messages.append(Message(str.encode("Some Value")))
    return messages


def watcher():
    """
    This function is used to watch the side input directory for changes.
    """
    path = sideinputsdk.SideInput.SIDE_INPUT_DIR_PATH
    for changes in watch(path):
        print(changes)


if __name__ == "__main__":
    """
    This function is used to start the GRPC server and the watcher thread.
    """
    daemon = Thread(target=watcher, daemon=True, name="Monitor")
    grpc_server = Mapper(handler=my_handler)
    thread_server = Thread(target=grpc_server.start, daemon=True, name="GRPC Server")
    daemon.start()
    thread_server.start()
    thread_server.join()
    daemon.join()
