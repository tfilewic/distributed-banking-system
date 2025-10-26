"""
client.py
CSE 531 - gRPC Project
tfilewic
2025-10-26

Runs customer events from input.json and writes output.json.
"""

import json
from utilities import import_file, OUTPUT_FILE

from customer import Customer
from time import sleep

PROPAGATION_DELAY = 0.1

def filter_output(responses):
    events = responses.get("recv", [])
    responses["recv"] = [
        event for event in events 
        if event.get("result") != "fail"
    ]
    return responses


def process_customers() -> list[dict]:

    data = import_file()
    output = []

    for item in data:
        if item.get("type") == "customer":
            id = item.get("id")
            events = item.get("events")
            customer = Customer(id, events)
            customer.createStub()
            responses = customer.executeEvents()
            responses = filter_output(responses)
            print(json.dumps(responses, indent=2))
            sleep(PROPAGATION_DELAY)
            output.append(responses)

    return output

def export(data: list[dict]):
    with open(OUTPUT_FILE, 'w') as file:
        json.dump(data, file, indent=2)


if __name__ == "__main__":

    output = process_customers()
    export(output)