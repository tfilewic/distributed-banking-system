"""
client.py
CSE 531 - gRPC Project
tfilewic
2025-11-19

Runs Customer events from input and writes output.
"""

import json
import grpc
import itertools
from utilities import create_channel, import_file, OUTPUT_FILE
from customer import Customer
from time import sleep
import banks_pb2
import banks_pb2_grpc

PROPAGATION_DELAY = 0.1



def process_customers(data) -> list[dict]:
    """
    Executes all customer event sequences from the input file.

    Reads customer entries from the input JSON, runs each customer's
    defined events sequentially, filters failed transactions, prints
    results for debugging, and collects all responses for export.

    Returns:
        list[dict]: A list of sent requests from each customer.
    """
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


def get_branch_events(data):
    """
    Retrieves the logged events from each branch.

    Args:
        data (list[dict]): The parsed input JSON containing customer and branch definitions.

    Returns:
        list[dict]: A list of dictionaries, one per branch, each containing
                    the branch id, type, and its list of logged events.
    """
    branch_events = []
    branches = [item["id"] for item in data if item.get("type") == "branch"]    #collect all branch ids
    for branch in branches:
        channel =  create_channel(branch)
        stub =  banks_pb2_grpc.RPCStub(channel)
        log = stub.Get_Log(banks_pb2.Query())

        #convert protobuf to dict
        events = []
        for event in log.events:
            events.append({
                "customer-request-id": event.customer_request_id,
                "logical_clock": event.logical_clock,
                "interface": event.interface,
                "comment": event.comment
            })

        #add to list
        branch_events.append({
            "id" : branch,
            "type" : "branch",
            "events" : events
            })
        
    return branch_events


def calculate_event_chain(customer_events, branch_events):
    """
    Builds the event chains for the output file.

    Args:
        customer_events (list): List of customer dictionaries.
        branch_events (list): List of branch dictionaries.

    Returns:
        list: A list of event chains, where each chain is a list of events
        grouped by customer-request-id and sorted by logical clock.
    """
    #flatten events so they contain top level fields
    flattened_events = []
    for source in customer_events + branch_events:
        for event in source["events"]:
            flattened_events.append({
                "id" : source["id"],
                "customer-request-id" : event["customer-request-id"],
                "type" : source["type"],
                "logical_clock" : event["logical_clock"],
                "interface" : event["interface"],
                "comment" : event["comment"]
            })

    #group events by request id 
    chains = {}
    for event in flattened_events:
        request_id = event["customer-request-id"]
        if request_id not in chains:
            chains[request_id] = []
        chains[request_id].append(event)

    #sort chains by logical clock:
    for request_id in chains:
        chains[request_id].sort(key=lambda entry: entry["logical_clock"])

    #convert dict to list
    #return list(chains.values())
    return list(itertools.chain.from_iterable(chains.values()))


def export(customer_events, branch_events, event_chain):
    """
    Writes the processed customer responses to the output JSON file.

    Args:
        data (list[dict]): List of customer response dictionaries to save.
    """
    with open(OUTPUT_FILE, 'w') as file:
        data = customer_events + branch_events + event_chain
        json.dump(data, file, indent=2)


def run():
    """
    Main client function.
    """
    data = import_file()    #load input
    customer_events = process_customers(data)   #run customers
    branch_events = get_branch_events(data) #fetch branch logs
    event_chain = calculate_event_chain(customer_events, branch_events) #build output
    export(customer_events, branch_events, event_chain) #save output


#run when script called directly
if __name__ == "__main__":  
    try:
        run()
    except grpc.RpcError as e:
        print(f"ERROR: {e.details()}")
        print("Ensure all branch servers are running before starting the client.")
        exit(1)