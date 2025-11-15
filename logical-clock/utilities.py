"""
utilties.py
CSE 531 - gRPC Project
tfilewic
2025-10-26

Shared constants and helper functions.
"""

import grpc
import json
import sys

BASE_PORT = 50000   #base port used to assign ports sequentially
QUERY = "query"
DEPOSIT = "deposit"
WITHDRAW = "withdraw"
INPUT_FILE = "input.json"
OUTPUT_FILE = "output.json"


def get_port(id: int) -> int:
    """
    Generates a port based on a customer or branch id.

    Args:
        id (int): Unique process ID.

    Returns:
        int: Port number.
    """
    return BASE_PORT + id


def create_channel(id: int) -> grpc.Channel:
    """
    Creates an insecure gRPC channel to the branch with the given ID.

    Args:
        id (int): Branch ID to connect to.

    Returns:
        grpc.Channel: gRPC communication channel to the target branch.
    """
    port = get_port(id)
    return grpc.insecure_channel(f"localhost:{port}")


def import_file() -> dict:
    """
    Loads and parses the input JSON file.

    Returns:
        dict: Parsed JSON data, or an empty dictionary if the file is missing or invalid.
    """
    filename = sys.argv[1] if len(sys.argv) > 1 else INPUT_FILE  #use CLI arg if given, else default file

    try:
        with open(filename, 'r') as file:
            data = json.load(file)  #parse JSON content
            return data #return parsed data
    except FileNotFoundError:   #handle missing file
        print(f"Error: File '{filename}' not found.")
        return {}
    except json.JSONDecodeError:    #handle invalid JSON syntax
        print(f"Error: Invalid JSON in '{filename}'.")
        return {}