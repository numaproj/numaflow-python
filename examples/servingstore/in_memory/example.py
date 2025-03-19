from pynumaflow.servingstore import (
    ServingStorer,
    PutDatum,
    GetDatum,
    StoredResult,
    ServingStoreServer,
    Payload,
)


class InMemoryStore(ServingStorer):
    def __init__(self):
        self.store = {}

    def put(self, datum: PutDatum):
        req_id = datum.id
        print("Received Put request for ", req_id)
        if req_id not in self.store:
            self.store[req_id] = []

        cur_payloads = self.store[req_id]
        for x in datum.payloads:
            cur_payloads.append(Payload(x.origin, x.value))
        self.store[req_id] = cur_payloads

    def get(self, datum: GetDatum) -> StoredResult:
        req_id = datum.id
        print("Received Get request for ", req_id)
        resp = []
        if req_id in self.store:
            resp = self.store[req_id]
        return StoredResult(id_=req_id, payloads=resp)


if __name__ == "__main__":
    grpc_server = ServingStoreServer(InMemoryStore())
    grpc_server.start()
