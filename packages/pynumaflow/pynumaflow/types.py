from typing import NewType

import grpc

NumaflowServicerContext = NewType(
    "NumaflowServicerContext", grpc.aio.ServicerContext | grpc.ServicerContext
)
