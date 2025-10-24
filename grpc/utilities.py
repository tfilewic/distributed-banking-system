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


def create_channel(id: int):
    port = BASE_PORT + id
    return grpc.insecure_channel(f"localhost:{port}")
