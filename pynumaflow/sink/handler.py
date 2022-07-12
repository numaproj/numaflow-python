import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any, Tuple, List

import msgpack
from aiohttp import web

from pynumaflow._constants import (
    SINK_SOCK_PATH,
    APPLICATION_JSON,
    APPLICATION_MSG_PACK,
)
from pynumaflow._tools import Singleton
from pynumaflow.decoder import NumaflowJSONDecoder, msgpack_decoder
from pynumaflow.exceptions import InvalidContentTypeError
from pynumaflow.sink._dtypes import Responses, Message

UDSinkCallable = Callable[[List[Message], Any], Responses]
_LOGGER = logging.getLogger(__name__)


class HTTPSinkHandler(metaclass=Singleton):
    """
    Provides an interface to write a User Defined Sink (UDSink)
    which will be exposed over HTTP.

    Args:
        handler: Function callable following the type signature of UDSinkCallable
        sock_path: Path to the UNIX Domain Socket
        max_workers: Max number of threads to be spawned

    Example invocation:
    >>> from pynumaflow.sink import Responses, Response, Message
    >>> # mysinkfunc is a callable following the UDSinkCallable signature
    >>> def mysinkfunc(messages: List[Message], __) -> Responses:
    ...     responses = Responses()
    ...     for msg in messages:
    ...         responses.append(Response.as_success(msg.id))
    ...     return responses
    ...
    >>> http_handler = HTTPSinkHandler(mysinkfunc, sock_path="/tmp/uds.sock")
    >>> http_handler.start()
    """

    app = web.Application()

    def __init__(
        self, handler: UDSinkCallable, sock_path: str = SINK_SOCK_PATH, max_workers: int = 50
    ):
        self.__handler = handler
        self.sock_path = sock_path
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        self.app.add_routes(
            [
                web.get("/ready", self.ready_handler),
                web.post("/messages", self.messages_handler),
            ]
        )
        self.app.on_shutdown.append(self.__on_shutdown)

    @staticmethod
    async def ready_handler(*_) -> web.Response:
        """
        End point for readiness check.

        Returns:
            An aiohttp response object
        """
        _LOGGER.info("READY")
        return web.Response(status=204)

    @staticmethod
    def decode(data: bytes, content_type: str) -> List[Message]:
        """
        Decode the bytes array into a list of Sink Message instances.
        Args:
            data: encoded bytes data of Message instances
            content_type: type of content used in the HTTP request

        Returns:
            List of Sink Message objects.

        Raises:
            InvalidContentTypeError: if the content type is not one of json or msgpack
        """
        if content_type == APPLICATION_JSON:
            messages = json.loads(data.decode("utf-8"), cls=NumaflowJSONDecoder)
        elif content_type == APPLICATION_MSG_PACK:
            messages = msgpack.unpackb(data, object_pairs_hook=msgpack_decoder)
        else:
            raise InvalidContentTypeError(f"Invalid Content-Type given: {content_type}")
        return [Message(**params) for params in messages]

    async def messages_handler(self, request: web.Request) -> web.Response:
        """
        End point for passing data through the User Defined Sink callable.
        Args:
            request: aiohttp Request object

        Returns:
            aiohttp Response object
        """
        content_type = request.headers.get("Content-Type")
        try:
            data: bytes = await request.content.read()
            messages = self.decode(data, content_type)

            loop = asyncio.get_running_loop()
            responses: Responses = await loop.run_in_executor(
                self.executor, self.__handler, messages, {}
            )

            out_msgs = responses.dumps(content_type)
            return web.Response(body=out_msgs, status=200, content_type=content_type)

        except Exception as err:
            exception_type = type(err).__name__
            error_msg = f"Got an unexpected exception: {exception_type}, {repr(err)}"
            _LOGGER.exception(error_msg)
            return web.Response(body=error_msg, status=500)

    async def __on_shutdown(self, *_) -> None:
        _LOGGER.info("SDK Server shutting down.")
        self.executor.shutdown()

    async def __start_app(self) -> Tuple[web.AppRunner, web.UnixSite]:
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.UnixSite(runner, self.sock_path)
        await site.start()
        _LOGGER.info("Server starting at socket: %s with pid %s", site.name, os.getpid())
        return runner, site

    def start(self) -> None:
        """
        Starts the server on the given UNIX socket.
        """
        loop = asyncio.get_event_loop()
        runner, site = loop.run_until_complete(self.__start_app())

        _LOGGER.info("Running ThreadPool with max workers: %s", self.max_workers)

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            loop.run_until_complete(runner.cleanup())
