"""
utilties.py
CSE 531 - gRPC Project
tfilewic
2025-10-23

Shared constants and helper functions.
"""

import grpc
import json
import sys

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