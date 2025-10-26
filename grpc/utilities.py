"""
utilties.py
CSE 531 - gRPC Project
tfilewic
2025-10-23

Shared constants and helper functions.
"""

import grpc

BASE_PORT = 50000 
QUERY = "query"
DEPOSIT = "deposit"
WITHDRAW = "withdraw"
INPUT_FILE = "grpc_input.json"
OUTPUT_FILE = "output.json"


def get_port(id: int):
    return BASE_PORT + id

def create_channel(id: int):
    port = get_port(id)
    return grpc.insecure_channel(f"localhost:{port}")
