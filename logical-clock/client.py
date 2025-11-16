"""
client.py
CSE 531 - gRPC Project
tfilewic
2025-10-26

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
        list[dict]: A list of sent requests from each customer.
    """
    #load input data from JSON file
    data = import_file()
    #initialize list to store results
    customer_events = []

    #process all customer entries
    for item in data:
        if item.get("type") == "customer":
            id = item.get("id")
            events = item.get("customer-requests")
            customer = Customer(id, events)
            customer.createStub()
            customer_log = customer.executeEvents()
            sleep(PROPAGATION_DELAY)    #wait for branch propagation
            customer_events.append(customer_log)

    return customer_events


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
        customer_events = process_customers()
        # branch_events = get_branch_events()
        # event_chain = calculate_event_chain(customer_events, branch_events)
        # export(customer_events, branch_events, event_chain)
    except grpc.RpcError as e:
        print(f"ERROR: {e.details()}")
        print("Ensure all branch servers are running before starting the client.")
        exit(1)