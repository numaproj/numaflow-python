from typing import Union, NewType
import grpc

NumaflowServicerContext = NewType(
    "NumaflowServicerContext", Union[grpc.aio.ServicerContext, grpc.ServicerContext]
)
