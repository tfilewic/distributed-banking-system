"""
client.py
CSE 531 - CCC Monotonic Writes Project
tfilewic
2025-11-27

Runs Customer events from input and writes output.
"""

import json
import grpc
from utilities import import_file, OUTPUT_FILE
from customer import Customer

PROPAGATION_DELAY = 0.1

def process_customers() -> list[dict]:
    """
    Executes all customer event sequences from the input file.

    Reads customer entries from the input JSON, runs each customer's
    defined events sequentially, and collects all responses for export.

    Returns:
        list[dict]: A list of processed customer response dictionaries.
    """
    
    data = import_file()    #load input data from JSON file
    output = [] #initialize list to store results

    #process all customer entries
    for item in data:
        if item.get("type") == "customer":
            id = item.get("id")
            events = item.get("events")
            customer = Customer(id, events)
            responses = customer.executeEvents()
            output.extend(responses)

    return output


def export(data: list[dict]):
    """
    Writes the processed customer responses to the output JSON file.

    Args:
        data (list[dict]): List of customer response dictionaries to save.
    """

    with open(OUTPUT_FILE, 'w') as file:
        json.dump(data, file, indent=2)


#run when script called directly
if __name__ == "__main__":  
    try:
        output = process_customers()
        export(output)
    except grpc.RpcError as e:
        print(f"ERROR: {e.details()}")
        print("Ensure all branch servers are running before starting the client.")
        exit(1)