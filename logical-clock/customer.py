"""
customer.py
CSE 531 - Logical Clock Project
tfilewic
2025-11-14

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
        # a list of received messages used for debugging purpose
        self.recvMsg = list() #TODO replace with event log
        # pointer for the stub
        self.stub = None
        # logical clock
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
            dict: A dictionary containing the received responses for this customer id.
        """

        #process all events
        for event in self.events:
            self.clock += 1
            interface = event["interface"]
            entry = {"interface" : interface}

            #handle deposits
            if interface == DEPOSIT:
                request = banks_pb2.TransactionRequest(id=self.id, amount=event["money"], request_id=event["customer-request-id"], clock=self.clock)
                response = self.stub.Deposit(request)
                entry["result"] = response.result

            #handle withdrawals
            elif interface == WITHDRAW:
                request = banks_pb2.TransactionRequest(id=self.id, amount=-event["money"], request_id=event["customer-request-id"], clock=self.clock)
                response = self.stub.Withdraw(request)
                entry["result"] = response.result

            #ignore unsupported types
            else: 
                continue          
            
            #add response to recvMsg
            self.recvMsg.append(entry) #TODO replace with event log
        
        #return responses
        return {"id": self.id, "recv": self.recvMsg}