"""
client.py
CSE 531 - CCC Read Your Writes Project
tfilewic
2025-11-26

Runs Customer events from input and writes output.
"""

import json
import grpc
from utilities import import_file, OUTPUT_FILE
from customer import Customer
from time import sleep

PROPAGATION_DELAY = 0.1


def filter_output(responses: dict) -> dict:
    """
    Removes failed transaction events from a customer's response list.

    Args:
        responses (dict): The customer's full response containing a "recv" list of event results.

    Returns:
        dict: The same response dictionary with all events having result == "fail" removed.
    """
    events = responses.get("recv", [])   #get list of events from response

    responses["recv"] = [   #replace recv list with filtered events 
        event for event in events 
        if event.get("result") != "fail"
    ]
    return responses


def process_customers() -> list[dict]:
    """
    Executes all customer event sequences from the input file.

    Reads customer entries from the input JSON, runs each customer's
    defined events sequentially, filters failed transactions, prints
    results for debugging, and collects all responses for export.

    Returns:
        list[dict]: A list of processed customer response dictionaries.
    """
    #load input data from JSON file
    data = import_file()
    #initialize list to store results
    output = []

    #process all customer entries
    for item in data:
        if item.get("type") == "customer":
            id = item.get("id")
            events = item.get("events")
            customer = Customer(id, events)
            responses = customer.executeEvents()
            responses = filter_output(responses)    #filter out "fail"
            sleep(PROPAGATION_DELAY)    #wait for branch propagation
            output.append(responses)

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