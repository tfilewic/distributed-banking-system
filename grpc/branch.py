"""
branch.py
CSE 531 - gRPC Project
tfilewic
2025-10-26

Branch server logic and RPC handlers.
"""

import banks_pb2
import banks_pb2_grpc
from utilities import create_channel


class Branch(banks_pb2_grpc.RPCServicer):
    """
    Represents a branch server.
    Handles local balance updates and propagates changes to peer branches.
    """
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
        """
        Propagates a deposit or withdrawal request to all other branches.

        Args:
            request (banks_pb2.TransactionRequest): The transaction to propagate.
        """
        for branchStub in self.stubList:
            if (request.amount > 0):
                branchStub.Propagate_Deposit(request)
            elif (request.amount < 0):
                branchStub.Propagate_Withdraw(request)


    """
    Since the assignment spec requires a central handler, all RPC interface methods delegate to MsgDelivery.
    """
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


    def MsgDelivery(self, request, context):
        """
        Central handler for all incoming gRPC requests.

        Determines request type and processes accordingly.

        Args:
            request: The gRPC request message (TransactionRequest or BalanceRequest).
            context: The gRPC context object for the call.

        Returns:
            banks_pb2.TransactionResponse or banks_pb2.BalanceResponse:
            The appropriate response message containing result or balance.
        """
        if isinstance(request, banks_pb2.TransactionRequest):   #handle deposit or withdraw
            response = banks_pb2.TransactionResponse()
            if (request.amount + self.balance < 0): #return fail on insufficient funds
                response.result = "fail"
            else:
                self.balance += request.amount  #update local balance

                if (request.id == self.id): #propagate customer requests
                    self.propagate(request)

                response.result = "success"

        elif isinstance(request, banks_pb2.BalanceRequest): #handle balance request
            response = banks_pb2.BalanceResponse()
            response.balance = self.balance        
        
        return response


