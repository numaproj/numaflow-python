import asyncio
import sys

from pynumaflow._constants import (
    NUM_THREADS_DEFAULT,
    MAX_MESSAGE_SIZE,
    MAX_NUM_THREADS,
    SOURCE_TRANSFORMER_SOCK_PATH,
    SOURCE_TRANSFORMER_SERVER_INFO_FILE_PATH,
    _LOGGER,
)
from pynumaflow._metadata import UserMetadata as _PyUserMetadata, SystemMetadata as _PySystemMetadata
from pynumaflow.shared.server import NumaflowServer
from pynumaflow.sourcetransformer._dtypes import (
    SourceTransformAsyncCallable,
    Datum as _PyDatum,
)

# The Rust-backed source transformer engine. This module is compiled into the
# pynumaflow wheel via maturin (see Cargo.toml / pyproject.toml [tool.maturin]).
from pynumaflow._pynumaflow_rs import sourcetransformer as _rs


def _py_user_metadata_from_rs(rs_md) -> _PyUserMetadata:
    """Convert a Rust UserMetadata into the pure-Python UserMetadata."""
    md = _PyUserMetadata()
    for group in rs_md.groups():
        for key in rs_md.keys(group):
            md.add_key(group, key, rs_md.value(group, key))
    return md


def _py_system_metadata_from_rs(rs_md) -> _PySystemMetadata:
    """Convert a Rust SystemMetadata into the pure-Python (read-only) SystemMetadata."""
    data: dict[str, dict[str, bytes]] = {}
    for group in rs_md.groups():
        data[group] = {key: rs_md.value(group, key) for key in rs_md.keys(group)}
    return _PySystemMetadata(data)


def _rs_user_metadata_from_py(py_md: _PyUserMetadata):
    """Convert a pure-Python UserMetadata into a Rust UserMetadata."""
    rs_md = _rs.UserMetadata()
    for group in py_md.groups():
        rs_md.create_group(group)
        for key in py_md.keys(group):
            rs_md.add_kv(group, key, py_md.value(group, key))
    return rs_md


def _py_datum_from_rs(keys: list[str], rs_datum) -> _PyDatum:
    """Build the legacy pure-Python Datum from the Rust Datum handed in by the engine."""
    return _PyDatum(
        keys=keys,
        value=rs_datum.value,
        event_time=rs_datum.event_time,
        watermark=rs_datum.watermark,
        headers=rs_datum.headers,
        user_metadata=_py_user_metadata_from_rs(rs_datum.user_metadata),
        system_metadata=_py_system_metadata_from_rs(rs_datum.system_metadata),
    )


def _rs_messages_from_py(py_messages) -> "_rs.Messages":
    """Convert the user-returned (pure-Python) Messages into a Rust Messages."""
    rs_messages = _rs.Messages()
    for msg in py_messages:
        rs_msg = _rs.Message(
            msg.value,
            msg.event_time,
            keys=list(msg.keys) if msg.keys else None,
            tags=list(msg.tags) if msg.tags else None,
            user_metadata=_rs_user_metadata_from_py(msg.user_metadata)
            if msg.user_metadata is not None
            else None,
        )
        rs_messages.append(rs_msg)
    return rs_messages


class SourceTransformAsyncServer(NumaflowServer):
    """
    Create a new Source Transformer Server instance backed by the Rust engine.

    This preserves the existing public API: construct with the handler and call
    the blocking ``start()``. Internally it drives the compiled Rust gRPC server
    while adapting the user handler so it continues to receive and return the
    pure-Python ``Datum`` / ``Messages`` / ``Message`` types.

    Args:
        source_transform_instance: The source transformer instance to be used for
            the Source Transformer UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to 4 and max capped at 16
        server_info_file: The path to the server info file
        shutdown_callback: Callable, executed after loop is stopped, before
                            cancelling any tasks. Useful for graceful shutdown.

    ```py
    import datetime
    import logging
    from pynumaflow.sourcetransformer import Messages, Message, Datum, SourceTransformAsyncServer

    january_first_2022 = datetime.datetime.fromtimestamp(1640995200)
    january_first_2023 = datetime.datetime.fromtimestamp(1672531200)


    async def my_handler(keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        event_time = datum.event_time
        messages = Messages()

        if event_time < january_first_2022:
            messages.append(Message.to_drop(event_time))
        elif event_time < january_first_2023:
            messages.append(
                Message(value=val, event_time=january_first_2022, tags=["within_year_2022"])
            )
        else:
            messages.append(
                Message(value=val, event_time=january_first_2023, tags=["after_year_2022"])
            )

        return messages


    if __name__ == "__main__":
        grpc_server = SourceTransformAsyncServer(my_handler)
        grpc_server.start()
    ```
    """

    def __init__(
        self,
        source_transform_instance: SourceTransformAsyncCallable,
        sock_path=SOURCE_TRANSFORMER_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=SOURCE_TRANSFORMER_SERVER_INFO_FILE_PATH,
        shutdown_callback=None,
    ):
        # Note: the Rust engine manages the gRPC transport itself, so
        # max_message_size / max_threads are accepted for API compatibility but
        # are not wired through here.
        self.sock_path = sock_path
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file
        self.shutdown_callback = shutdown_callback

        self.source_transform_instance = source_transform_instance
        self._error: BaseException | None = None

    async def _adapter(self, keys: list[str], rs_datum) -> "_rs.Messages":
        """Bridge between the Rust engine and the user's pure-Python handler."""
        datum = _py_datum_from_rs(keys, rs_datum)
        responses = await self.source_transform_instance(keys, datum)
        return _rs_messages_from_py(responses)

    def start(self) -> None:
        """
        Starter function for the async server. Blocks until the server shuts down.
        """
        try:
            asyncio.run(self.aexec())
        finally:
            if self.shutdown_callback is not None:
                self.shutdown_callback()
        if self._error:
            _LOGGER.critical("Server exiting due to UDF error: %s", self._error)
            sys.exit(1)

    async def aexec(self) -> None:
        """
        Starts the Rust-backed async gRPC server on the given UNIX socket.
        """
        server = _rs.SourceTransformAsyncServer(self.sock_path, self.server_info_file)
        _LOGGER.info("Async (Rust) GRPC Server listening on: %s", self.sock_path)
        try:
            await server.start(self._adapter)
        except asyncio.CancelledError:
            _LOGGER.info("Received cancellation, stopping server gracefully...")
            try:
                server.stop()
            except Exception:
                pass
