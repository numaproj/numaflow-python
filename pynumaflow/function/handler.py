import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from os import getpid, environ
from typing import Tuple, Callable, Any

from aiohttp import web

from pynumaflow._constants import (
    NUMAFLOW_UDF_CONTENT_TYPE,
    NUMAFLOW_MESSAGE_KEY,
    FUNCTION_SOCK_PATH,
)
from pynumaflow.exceptions import MissingHeaderError
from pynumaflow.function._dtypes import Messages
from pynumaflow._tools import Singleton

if environ.get("PYTHONDEBUG"):
    logging.basicConfig(level=logging.DEBUG)

_LOGGER = logging.getLogger(__name__)


UDFCallable = Callable[[bytes, bytes, Any], Messages]


class HTTPHandler(metaclass=Singleton):
    """
    Provides an interface to write User Defined Function (UDF)
    in Python which will be exposed over HTTP.

    Example invocation:
    >>> # myhandlerfunc is a callable following the UDFCallable signature
    >>> def myhandlerfunc(key, val, _):
    ...     msgs = Messages()
    ...     msgs.append(val)
    ...     return msgs
    ...
    >>> http_handler = HTTPHandler(myhandlerfunc)
    >>> http_handler.start()
    Args:
        handler: Handler Callable following the UDFCallable signature
        sock_path: Path to the UNIX Domain Socket
        max_workers: Maximum number of threads for ThreadPoolExecutor
    """

    def __init__(self, handler: UDFCallable, sock_path=FUNCTION_SOCK_PATH, max_workers=50):
        self.__handler: UDFCallable = handler
        self.sock_path = sock_path
        self.max_workers = max_workers
        self.udf_content_type = environ.get(NUMAFLOW_UDF_CONTENT_TYPE)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        self.app = web.Application()
        self.app.add_routes(
            [
                web.get("/ready", self.ready_handler),
                web.post("/messages", self.messages_handler),
            ]
        )
        self.app.on_shutdown.append(self.__on_shutdown)

    @staticmethod
    async def ready_handler(*_) -> web.Response:
        _LOGGER.info("READY")
        return web.Response(status=204)

    async def messages_handler(self, request: web.Request) -> web.Response:
        """
        End point for passing data through the User Defined Function (UDF) callable.
        Args:
            request: aiohttp Request object

        Returns:
            aiohttp Response object
        """
        key = request.headers.get(NUMAFLOW_MESSAGE_KEY, "")
        if key is None:
            raise MissingHeaderError(f"Missing header {NUMAFLOW_MESSAGE_KEY}, key: {key}")

        try:
            in_msg = await request.content.read()

            loop = asyncio.get_running_loop()
            messages = await loop.run_in_executor(
                self.executor, self.__handler, key.encode(), in_msg, {}
            )

            out_msgs = messages.dumps(self.udf_content_type)
            return web.Response(body=out_msgs, status=200, content_type=self.udf_content_type)

        except Exception as err:
            exception_type = type(err).__name__
            error_msg = f"Got an unexpected exception: {exception_type}, {err}"
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
        _LOGGER.info("Server starting at socket: %s with pid %s", site.name, getpid())
        return runner, site

    def start(self) -> None:
        loop = asyncio.get_event_loop()
        runner, site = loop.run_until_complete(self.__start_app())

        _LOGGER.info("Running ThreadPool with max workers: %s", self.max_workers)

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            loop.run_until_complete(runner.cleanup())
