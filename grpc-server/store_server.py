import sys
import grpc
import concurrent.futures
import logging

import store
import store_pb2
import store_pb2_grpc

class Store(store_pb2_grpc.StoreServicer):
    def __init__(self):
        self.store = store.Store(sys.argv[1], 1024 * 1024)
    
    def Get(self, request: store_pb2.GetRequest, context):
        res = self.store.get(request.key)

        # TODO(Apaar): Can we just pass along the bytearray as-is?
        return store_pb2.GetReply(value=bytes(res) if res else None)

    def Set(self, request: store_pb2.SetRequest, context):
        self.store.set(request.key, request.value)

        return store_pb2.SetReply()

def serve():
    port = sys.argv[2]
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))

    store_pb2_grpc.add_StoreServicer_to_server(Store(), server)

    server.add_insecure_port("[::]:" + port)
    server.start()

    logging.info("Server started, listening on port %s", port)
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    serve()