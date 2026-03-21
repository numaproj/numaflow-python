from collections.abc import Iterator

from pynumaflow.sinker import Response, Responses, Sinker, Datum, Message, UserMetadata


def test_as_success():
    succ_response = Response.as_success("2")
    assert succ_response.success


def test_as_failure():
    _response = Response.as_failure("3", "RuntimeError encountered!")
    assert not _response.success


def test_as_fallback():
    _response = Response.as_fallback("4")
    assert not _response.success
    assert _response.fallback


def test_as_on_success():
    _response = Response.as_on_success("5", Message(b"value", ["key"], UserMetadata()))
    assert not _response.success
    assert not _response.fallback
    assert _response.on_success


def test_responses():
    resps = Responses(
        Response.as_success("2"),
        Response.as_failure("3", "RuntimeError encountered!"),
        Response.as_fallback("5"),
    )
    resps.append(Response.as_success("4"))
    resps.append(Response.as_on_success("6", Message(b"value", ["key"], UserMetadata())))
    resps.append(Response.as_on_success("7", None))
    assert 6 == len(resps)

    for resp in resps:
        assert isinstance(resp, Response)

    assert resps[0].id == "2"
    assert resps[1].id == "3"
    assert resps[2].id == "5"
    assert resps[3].id == "4"
    assert resps[4].id == "6"
    assert resps[5].id == "7"

    assert (
        "[Response(id='2', success=True, err=None, fallback=False, "
        "on_success=False, on_success_msg=None), "
        "Response(id='3', success=False, err='RuntimeError encountered!', "
        "fallback=False, on_success=False, on_success_msg=None), "
        "Response(id='5', success=False, err=None, fallback=True, "
        "on_success=False, on_success_msg=None), "
        "Response(id='4', success=True, err=None, fallback=False, "
        "on_success=False, on_success_msg=None), "
        "Response(id='6', success=False, err=None, fallback=False, "
        "on_success=True, on_success_msg=Message(_keys=['key'], _value=b'value', "
        "_user_metadata=UserMetadata(_data={}))), "
        "Response(id='7', success=False, err=None, fallback=False, "
        "on_success=True, on_success_msg=None)]" == repr(resps)
    )


class ExampleSinkClass(Sinker):
    def handler(self, datums: Iterator[Datum]) -> Responses:
        results = Responses()
        results.append(Response.as_success("test_message"))
        return results


def test_sink_class_call():
    """Test that the __call__ functionality for the class works,
    ie the class instance can be called directly to invoke the handler function
    """
    sinker_instance = ExampleSinkClass()
    # make a call to the class directly
    ret = sinker_instance(None)
    assert "test_message" == ret[0].id
    # make a call to the handler
    ret_handler = sinker_instance.handler(None)
    # Both responses should be equal
    assert ret[0] == ret_handler[0]
