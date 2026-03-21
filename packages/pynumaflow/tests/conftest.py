"""
Root conftest.py — shared pytest fixtures and helpers for all test modules.

Provides helpers for common gRPC testing patterns that are duplicated across
sync, multiproc, and async test files.
"""


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
