def is_valid_handshake(req):
    """Check if the handshake message is valid."""
    return req.handshake and req.handshake.sot
