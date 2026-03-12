import pytest

from pynumaflow._validate import _validate_message_fields


class TestValidateMessageFields:
    def test_invalid_value_type_raises(self):
        with pytest.raises(TypeError, match="Message 'value' must be bytes, got str"):
            _validate_message_fields("not bytes", None, None)

    def test_invalid_keys_type_raises(self):
        with pytest.raises(TypeError, match="Message 'keys' must be a list of strings"):
            _validate_message_fields(None, "not-a-list", None)

    def test_invalid_keys_element_type_raises(self):
        with pytest.raises(TypeError, match="Message 'keys' must be a list of strings"):
            _validate_message_fields(None, [1, 2], None)

    def test_invalid_tags_type_raises(self):
        with pytest.raises(TypeError, match="Message 'tags' must be a list of strings"):
            _validate_message_fields(None, None, "not-a-list")

    def test_invalid_tags_element_type_raises(self):
        with pytest.raises(TypeError, match="Message 'tags' must be a list of strings"):
            _validate_message_fields(None, None, [1, 2])

    def test_valid_inputs_no_error(self):
        _validate_message_fields(b"data", ["key1"], ["tag1"])

    def test_all_none_no_error(self):
        _validate_message_fields(None, None, None)
