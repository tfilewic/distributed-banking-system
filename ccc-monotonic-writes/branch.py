"""
branch.py
CSE 531 - CCC Monotonic Writes Project
tfilewic
2025-11-27

Branch server logic and RPC handlers.
"""

import banks_pb2
import banks_pb2_grpc
from utilities import create_channel
from time import sleep


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
        # write ids this branch has performed
        self.writeset = set()
        # next write_id to assign for client writes
        self.next_write_id = self.id * 1000

        # add all branch stubs to stub list 
        for branch in branches:
            if branch != self.id:
                channel =  create_channel(branch)
                self.stubList.append(banks_pb2_grpc.RPCStub(channel))


    def propagate(self, amount, write_id, writeset):
        """
        Propagates a deposit or withdrawal request to all other branches.

        Args:
            amount (int): The amount to apply.
            write_id (int): The unique ID of the write operation.
            writeset (list[int]): IDs of all prior client writes this write depends on.
        """
        propagation_request = banks_pb2.PropagationRequest(amount=amount, write_id=write_id, writeset=writeset)

        for branchStub in self.stubList:
            if (propagation_request.amount > 0):
                branchStub.Propagate_Deposit(propagation_request)
            elif (propagation_request.amount < 0):
                branchStub.Propagate_Withdraw(propagation_request)

    def wait_for_writes(self, client_writeset):
        """
        Blocks until this branch has applied all writes in the client's writeset.

        Args:
            client_writeset (iterable): The set of write IDs the client has already completed.
        """
        client_writes = set(client_writeset)
        while not client_writes.issubset(self.writeset):
            sleep(0.01)

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
            request: The gRPC request message (TransactionRequest, PropagationRequest, or BalanceRequest).
            context: The gRPC context object for the call.

        Returns:
            banks_pb2.TransactionResponse or banks_pb2.BalanceResponse:
            The appropriate response message containing result or balance.
        """
        if isinstance(request, banks_pb2.TransactionRequest):   #handle customer deposit or withdraw
            self.wait_for_writes(request.writeset) #enforce monotonic-writes

            response = banks_pb2.TransactionResponse()
            if (request.amount + self.balance < 0): #return fail on insufficient funds
                response.write_id = 0
            else:
                self.balance += request.amount  #update local balance

                write_id = self.next_write_id   #generate write id
                self.next_write_id += 1
                self.writeset.add(write_id)

                self.propagate(request.amount, write_id, self.writeset)   #progate writes

                response.write_id = write_id

        elif isinstance(request, banks_pb2.PropagationRequest):   #handle propagation
            self.wait_for_writes(request.writeset) #enforce monotonic-writes
            response = banks_pb2.TransactionResponse()

            if request.write_id not in self.writeset:  #idempotently update branch balance
                self.balance += request.amount
                self.writeset.add(request.write_id)

            response.write_id = request.write_id    

        elif isinstance(request, banks_pb2.BalanceRequest): #handle balance request
            response = banks_pb2.BalanceResponse()
            response.balance = self.balance        
        
        return response


