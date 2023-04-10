import grpc
from typing import Union, NewType

NumaflowServicerContext = NewType(
    "NumaflowServicerContext", Union[grpc.aio.ServicerContext, grpc.ServicerContext]
)
