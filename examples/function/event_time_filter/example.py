import datetime
from pynumaflow.function import MessageTs, MessageT, Datum, UserDefinedFunctionServicer

def mapt_handler(key: str, datum: Datum) -> MessageTs:
    val = datum.value
    new_event_time = datetime.time()
    _ = datum.watermark
    message_t_s = MessageTs(MessageT.to_vtx(key, val, new_event_time))
    return message_t_s

if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(mapt_handler=mapt_handler)
    grpc_server.start()
