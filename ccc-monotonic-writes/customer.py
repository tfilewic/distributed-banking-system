"""
customer.py
CSE 531 - CCC Monotonic Writes Project
tfilewic
2025-11-27

Customer client logic and event execution.
"""

import banks_pb2
import banks_pb2_grpc
from utilities import create_channel, QUERY, WITHDRAW, DEPOSIT


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
        self.writeset = set()
        # a list of received messages used for debugging purpose
        self.received_messages = list()
        # map of stubs
        self.stubs = {}

    
    def createStub(self, branch_id: int):
        """
        Creates a gRPC stub for communicating with a branch and adds it to map.

        Args:
        branch_id (int): The ID of the branch to create a stub for.
        """
        channel =  create_channel(branch_id)
        stub =  banks_pb2_grpc.RPCStub(channel)
        self.stubs[branch_id] = stub

    def getStub(self, branch_id: int)  -> banks_pb2_grpc.RPCStub:
        """
        Returns a gRPC stub for the given branch; creates one if it doesn't exist.

        Args:
        branch_id (int): The ID of the branch whose stub is requested.

        Returns:
        RPCStub: The gRPC stub for the specified branch.
        """
        if branch_id not in self.stubs:
            self.createStub(branch_id)

        return self.stubs[branch_id]

    def executeEvents(self) -> dict:
        """
        Executes all customer events in order.

        Returns:
            dict: A dictionary containing the received query responses for this customer id.
        """
        output = []

        #process all events
        for event in self.events:
            branch = event["branch"]
            stub = self.getStub(branch)
            interface = event["interface"]
    
            #handle deposits and withdrawals
            if interface in {DEPOSIT, WITHDRAW}:
                money = event["money"] if interface == DEPOSIT else -event["money"]
                request = banks_pb2.TransactionRequest(amount=money, writeset=list(self.writeset))
                response = stub.Deposit(request) if (interface == DEPOSIT) else stub.Withdraw(request)
    
                write_id = response.write_id
                if (write_id != 0):
                    self.writeset.add(response.write_id)

            #handle balance queries
            elif interface == QUERY:
                request = banks_pb2.BalanceRequest()
                response = stub.Query(request)
                entry = {"id": event["id"], "balance": response.balance}
                output.append(entry) 

            #ignore unsupported types
            else: 
                continue          

        return output 
    1