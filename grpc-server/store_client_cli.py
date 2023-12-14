# TODO(Apaar): Move the client functionality into its own
# store_client.py file.
import sys

import grpc
import store_pb2
import store_pb2_grpc

def run():
    port = sys.argv[1]

    with grpc.insecure_channel("localhost:" + port) as channel:
        stub = store_pb2_grpc.StoreStub(channel)

        print("Connnected.")

        while True:
            cmd = input("> ").strip()

            if cmd == "get":
                key = input("key > ")

                response = stub.Get(store_pb2.GetRequest(key=bytes(key, "utf-8")))
                print(response)
            elif cmd == "set":
                key = input("key > ")
                value = input("value > ")

                response = stub.Set(
                    store_pb2.SetRequest(
                        key=bytes(key, "utf-8"), 
                        value=bytes(value, "utf-8")
                    )
                )

                print(response)
            elif cmd == "quit":
                break
            else:
                print("Unrecognized command.")
        
        print("Bye.")
                
if __name__ == '__main__':
    run()