from typing import Union, NewType, TypeVar, Type, Any
import grpc

from pynumaflow.exceptions import NoPublicConstructorError

NPC = TypeVar("NPC", bound="NoPublicConstructor")


class NoPublicConstructor(type):
    """Metaclass that ensures a private constructor

    If a class uses this metaclass like this:

        class SomeClass(metaclass=NoPublicConstructor):
            pass

    If you try to instantiate your class using (`SomeClass()`),
    a `NoPublicConstructorError` will be thrown.
    """

    def __call__(cls, *args, **kwargs):
        raise NoPublicConstructorError(
            "public constructor is not supported, please use class methods to create the object."
        )

    def _create(cls: Type[NPC], *args: Any, **kwargs: Any) -> NPC:
        return super().__call__(*args, **kwargs)


NumaflowServicerContext = NewType(
    "NumaflowServicerContext", Union[grpc.aio.ServicerContext, grpc.ServicerContext]
)
