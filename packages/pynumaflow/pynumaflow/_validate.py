def _validate_message_fields(value, keys, tags):
    """Validate common Message fields at construction time.

    Raises TypeError with a clear message pointing at the caller's code
    rather than letting bad types propagate to protobuf serialization.
    """
    if value is not None and not isinstance(value, bytes):
        raise TypeError(f"Message 'value' must be bytes, got {type(value).__name__}")
    if keys is not None:
        if not isinstance(keys, list) or not all(isinstance(k, str) for k in keys):
            raise TypeError(f"Message 'keys' must be a list of strings, got {keys!r}")
    if tags is not None:
        if not isinstance(tags, list) or not all(isinstance(t, str) for t in tags):
            raise TypeError(f"Message 'tags' must be a list of strings, got {tags!r}")
