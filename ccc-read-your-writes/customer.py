"""
customer.py
CSE 531 - CCC Read Your Writes Project
tfilewic
2025-11-27

Customer client logic and event execution.
"""

import banks_pb2
import banks_pb2_grpc
from utilities import create_channel, QUERY, WITHDRAW, DEPOSIT, SUCCESS, FAIL


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
        # set of writes this customer has completed
        self.write_set = set()
        # a list of received messages used for debugging purpose
        self.received_messages = list()
        # map of stubs
        self.stubs = {}

    
    def createStub(self, branch_id: int):
        """
        Creates a gRPC stub for communicating with a branch and adds it to map.
        """
        channel =  create_channel(branch_id)
        stub =  banks_pb2_grpc.RPCStub(channel)
        self.stubs[branch_id] = stub

    def getStub(self, branch_id: int):
        """
        Returns a gRPC stub for the given branch; creates one if it doesn't exist.
        """
        if branch_id not in self.stubs:
            self.createStub(branch_id)

        return self.stubs[branch_id]

    def executeEvents(self) -> dict:
        """
        Executes all customer events in order.

        Returns:
            dict: A dictionary containing the received responses for this customer id.
        """
        output = [] #DEBUG

        #process all events
        for event in self.events:
            branch = event["branch"]
            stub = self.getStub(branch)
            
            interface = event["interface"]
            entry = {"interface" : interface}

            #handle deposits and withdrawals
            if interface in {DEPOSIT, WITHDRAW}:
                amount = event["money"] if interface == DEPOSIT else -event["money"]
                request = banks_pb2.TransactionRequest(amount=amount, writeset=list(self.write_set))
                response = stub.Deposit(request) if (interface == DEPOSIT) else stub.Withdraw(request)
    
                write_id = response.write_id
                if (write_id == 0):
                    entry["result"] = FAIL
                else:
                    entry["result"] = SUCCESS
                    self.write_set.add(write_id)

            #handle balance queries
            elif interface == QUERY:
                request = banks_pb2.BalanceRequest(writeset=list(self.write_set))
                response = stub.Query(request)
                entry["branch"] = branch
                entry["balance"] = response.balance

            #ignore unsupported types
            else: 
                continue          
            
            output.append({"id": self.id, "recv": entry})
        
        #return responses
        return output
    
    '''            #add response to received_messages
            self.received_messages.append(entry)
        
        #return responses
        return "id": self.id, "recv": self.received_messages
    '''