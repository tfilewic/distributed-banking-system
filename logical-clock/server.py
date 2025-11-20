"""
server.py
CSE 531 - gRPC Project
tfilewic
2025-10-26

Starts Branch servers from input.json and registers services.
"""

import grpc
from concurrent import futures
from utilities import get_port, import_file
from branch import Branch
import banks_pb2_grpc

servers = []    #list of running gRPC servers


def start_branches(data : list) -> None:
    """
    Starts all branch servers defined in the input data.
    """
    branches = [item["id"] for item in data if item.get("type") == "branch"]    #collect all branch ids

    #process all branch entries
    for item in data:
        if item.get("type") == "branch":
            id = item.get("id")
            balance = item.get("balance")
            branch = Branch(id, balance, branches)  #create branch
            server = grpc.server(futures.ThreadPoolExecutor())  #create server
            banks_pb2_grpc.add_RPCServicer_to_server(branch, server)    #register branch service
            port = get_port(id) #generate port for this branch
            server.add_insecure_port(f"[::]:{port}") #bind to port
            server.start()  #start server
            servers.append(server)  #add to servers list


#run when script called directly
if __name__ == "__main__":
    data = import_file()    #load input
    start_branches(data)    #initialize and start all branch servers
    print("Servers started")
    #keep servers running until interrupted
    try:
        for server in servers:
            server.wait_for_termination()
    except KeyboardInterrupt:
        for server in servers:
            server.stop(0)
