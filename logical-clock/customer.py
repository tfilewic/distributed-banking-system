"""
customer.py
CSE 531 - Logical Clock Project
tfilewic
2025-11-15

Customer client logic and event execution.
"""

import banks_pb2
import banks_pb2_grpc
from utilities import create_channel, WITHDRAW, DEPOSIT


class Customer:
    """
    Represents a customer client that sends banking requests to its assigned branch.
    Handles stub creation and sequential event execution based on the input file.
    """
    
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # pointer for the stub
        self.stub = None
        # a list of sent messages
        self.log = []
        #logical clock
        self.clock = 0

    
    def createStub(self):
        """
        Creates a gRPC stub for communicating with the branch that shares this customer's ID.
        """
        channel =  create_channel(self.id)
        self.stub =  banks_pb2_grpc.RPCStub(channel)


    def executeEvents(self) -> dict:
        """
        Executes all customer events in order.

        Returns:
            dict: A dictionary containing the request log for this customer id.
        """

        #process all events
        for event in self.events:
            self.clock += 1
            
            interface = event["interface"]
            customer_request_id = event["customer-request-id"]
            entry = {
                "customer-request-id" : customer_request_id,
                "logical_clock" : self.clock,
                "interface" : interface,
                "comment" : f"event_sent from customer {self.id}"
            }
            self.log.append(entry)

            #handle deposits
            if interface == DEPOSIT:
                request = banks_pb2.TransactionRequest(id=self.id, amount=event["money"], request_id=customer_request_id, clock=self.clock)
                self.stub.Deposit(request)

            #handle withdrawals
            elif interface == WITHDRAW:
                request = banks_pb2.TransactionRequest(id=self.id, amount=-event["money"], request_id=customer_request_id, clock=self.clock)
                self.stub.Withdraw(request)

            #ignore unsupported types
            else: 
                continue          
        
        #return responses
        return {
            "id": self.id, 
            "type": "customer",
            "events": self.log
        }