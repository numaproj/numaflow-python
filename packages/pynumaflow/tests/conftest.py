"""
Root conftest.py — shared pytest fixtures and helpers for all test modules.

Provides helpers for common gRPC testing patterns that are duplicated across
sync, multiproc, and async test files.
"""

import asyncio
import logging
import threading

import grpc

_logger = logging.getLogger(__name__)


def start_async_server(loop, start_server_coro):
    """Start an async gRPC server on the given event loop and wait until it is ready.

    Args:
        loop: The asyncio event loop running in a background thread.
        start_server_coro: An awaitable that starts the server and returns
            a tuple of (grpc.aio.Server, sock_path).

    Returns:
        The grpc.aio.Server instance.
    """
    future = asyncio.run_coroutine_threadsafe(start_server_coro, loop=loop)
    server, sock_path = future.result(timeout=10)

    # Block until the server is accepting connections
    while True:
        try:
            with grpc.insecure_channel(sock_path) as channel:
                f = grpc.channel_ready_future(channel)
                f.result(timeout=10)
                if f.done():
                    break
        except grpc.FutureTimeoutError as e:
            _logger.error("error trying to connect to grpc server")
            _logger.error(e)

    return server


def create_async_loop():
    """Create a new asyncio event loop running in a daemon thread.

    Returns:
        The running event loop.
    """
    loop = asyncio.new_event_loop()

    def _run(lp):
        asyncio.set_event_loop(lp)
        lp.run_forever()

    thread = threading.Thread(target=_run, args=(loop,), daemon=True)
    thread.start()
    return loop


def teardown_async_server(loop, server):
    """Gracefully shut down an async gRPC server and its event loop.

    Stops the gRPC server, cancels any remaining tasks, then stops the loop.
    This prevents 'Task was destroyed but it is pending!' warnings.
    """

    async def _shutdown():
        await server.stop(grace=1)
        # Cancel any lingering tasks on this loop, excluding the current
        # _shutdown task itself to avoid recursive cancel chains.
        current = asyncio.current_task()
        tasks = [t for t in asyncio.all_tasks(loop) if not t.done() and t is not current]
        for task in tasks:
            task.cancel()
        # Await each cancelled task individually so a RecursionError in one
        # deeply-nested cancel chain does not prevent the others from being
        # reaped, and does not propagate up to the caller.
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, RecursionError, Exception):
                pass

    try:
        future = asyncio.run_coroutine_threadsafe(_shutdown(), loop=loop)
        future.result(timeout=10)
    except Exception as e:
        _logger.error("error during async server teardown: %s", e)
    finally:
        loop.call_soon_threadsafe(loop.stop)


def collect_responses(method):
    """Collect all responses from a grpc_testing stream method until exhausted.

    Replaces the repeated pattern:
        responses = []
        while True:
            try:
                resp = method.take_response()
                responses.append(resp)
            except ValueError as err:
                if "No more responses!" in err.__str__():
                    break

    Returns a list of response protos.
    """
    responses = []
    while True:
        try:
            resp = method.take_response()
            responses.append(resp)
        except ValueError as err:
            if "No more responses!" in str(err):
                break
    return responses


def drain_responses(method):
    """Drain all responses from a grpc_testing stream method, discarding them.

    Replaces the repeated pattern:
        while True:
            try:
                method.take_response()
            except ValueError:
                break

    Useful in shutdown tests where we only care about termination status.
    """
    while True:
        try:
            method.take_response()
        except ValueError:
            break


def send_test_requests(method, datums):
    """Send a list of test datums to a grpc_testing stream method and close.

    Replaces the repeated pattern:
        for d in test_datums:
            method.send_request(d)
        method.requests_closed()
    """
    for d in datums:
        method.send_request(d)
    method.requests_closed()
