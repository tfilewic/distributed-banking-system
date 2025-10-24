"""
branch.py
CSE 531 - gRPC Project
tfilewic
2025-10-23

Branch server logic and RPC handlers.
"""

import grpc
import banks_pb2
import banks_pb2_grpc
from utilities import create_channel


class Branch(banks_pb2_grpc.RPCServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
 
        # add all branch stubs to stub list 
        for branch in branches:
            if branch != self.id:
                channel =  create_channel(branch)
                self.stubList.append(banks_pb2_grpc.RPCStub(channel))


    def propagate(self, request):
        for branchStub in self.stubList:
            if (request.amount > 0):
                branchStub.Propagate_Deposit(request)
            elif (request.amount < 0):
                branchStub.Propagate_Withdraw(request)


    def Query(self, request, context):
        return self.MsgDelivery(request, context)

    def Deposit(self, request, context):
        return self.MsgDelivery(request, context)

    def Withdraw(self, request, context):
        return self.MsgDelivery(request, context)

    def Propagate_Deposit(self, request, context):
        return self.MsgDelivery(request, context)

    def Propagate_Withdraw(self, request, context):
        return self.MsgDelivery(request, context)


    # central handler for all incoming requests, per assignment spec
    def MsgDelivery(self, request, context):
       
        if hasattr(request, "amount"):
            response = banks_pb2.TransactionResponse()
            if (request.amount + self.balance < 0):
                response.result = "fail"
            else:
                self.balance += request.amount

                if (request.id == self.id):
                    self.propagate(request)

                response.result = "success"

        else:
            response = banks_pb2.BalanceResponse()
            response.balance = self.balance        
        
        return response


