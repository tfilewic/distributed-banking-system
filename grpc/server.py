"""
server.py
CSE 531 - gRPC Project
tfilewic
2025-10-25

Starts Branch servers from input.json and registers services.
"""


import sys
import json
import grpc
from concurrent import futures

from utilities import INPUT_FILE, get_port
from branch import Branch
import banks_pb2_grpc

servers = []

def import_file() -> dict:
    filename = sys.argv[1] if len(sys.argv) > 1 else INPUT_FILE

    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in '{filename}'.")
        return {}
    

def start_branches(data : list) -> None:
    branches = [item["id"] for item in data if item.get("type") == "branch"]

    for item in data:
        if item.get("type") == "branch":
            id = item.get("id")
            balance = item.get("balance")
            branch = Branch(id, balance, branches)
            server = grpc.server(futures.ThreadPoolExecutor())
            banks_pb2_grpc.add_RPCServicer_to_server(branch, server)
            port = get_port(id)
            server.add_insecure_port(f"[::]:{port}")
            server.start()
            #TODO debug
            print(f"starting branch server {id}")
            servers.append(server)


if __name__ == "__main__":
    data = import_file()
    start_branches(data)
    try:
        for server in servers:
            server.wait_for_termination()

    except KeyboardInterrupt:
        for server in servers:
            server.stop(0)
