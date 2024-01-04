class NoPublicConstructorError(TypeError):
    """Raise when using ClassName() to create objects while public constructor is not supported"""


class SocketError(Exception):
    """To raise an error while creating socket or setting its property"""
