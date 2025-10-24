"""
customer.py
CSE 531 - gRPC Project
tfilewic
2025-10-24

Customer client logic and event execution.
"""


import banks_pb2
import banks_pb2_grpc
from utilities import create_channel, QUERY, WITHDRAW, DEPOSIT


class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None

    
    def createStub(self):
        channel =  create_channel(self.id)
        self.stub =  banks_pb2_grpc.RPCStub(channel)


    def executeEvents(self):
        for event in self.events:
            interface = event["interface"]
            entry = {"interface" : interface}
            if interface == DEPOSIT:
                request = banks_pb2.TransactionRequest(id=self.id, amount=event["money"])
                response = self.stub.Deposit(request)
                entry["result"] = response.result
            elif interface == WITHDRAW:
                request = banks_pb2.TransactionRequest(id=self.id, amount=-event["money"])
                response = self.stub.Withdraw(request)
                entry["result"] = response.result
            elif interface == QUERY:
                request = banks_pb2.BalanceRequest(id=self.id)
                response = self.stub.Query(request)
                entry["balance"] = response.balance
            else: 
                continue          

            self.recvMsg.append(entry)
        
        return {"id": self.id, "recv": self.recvMsg}