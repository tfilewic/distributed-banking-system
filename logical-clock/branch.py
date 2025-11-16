"""
branch.py
CSE 531 - Logical Clock Project
tfilewic
2025-11-15

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
        self.stubList = {}
        # add all branch stubs to stub list 
        for branch in branches:
            if branch != self.id:
                channel =  create_channel(branch)
                self.stubList[branch] = banks_pb2_grpc.RPCStub(channel)
        # a list of received messages
        self.log = []
        # logical clock
        self.clock = 0


    def log_receipt(self, request):
        """
        Records a log entry for a received transaction request.

        Args:
            request (banks_pb2.TransactionRequest): The incoming transaction to log.
        """
        if (request.id == self.id):
            interface = ""
            comment = f"event_recv from customer {request.id}"
        else:
            interface = "propagate_"
            comment = f"event_recv from branch {request.id}"

        if (request.amount < 0):
            interface += "withdraw"
        else:
            interface += "deposit"

        event = {
            "customer-request-id": request.request_id, 
            "logical_clock": self.clock, 
            "interface": interface, 
            "comment": comment
        }

        self.log.append(event)


    def log_send(self, request, target_id, interface):
        """
        Records a log entry for a sent propagate event.

        Args:
            request (banks_pb2.TransactionRequest): The outgoing transaction being propagated.
            target_branch_id (int): The branch ID this event is being sent to.
            interface (str): The interface name ('propagate_deposit' or 'propagate_withdraw').
        """
        event = {
            "customer-request-id": request.request_id,
            "logical_clock": self.clock,
            "interface": interface,
            "comment": f"event_sent to branch {target_id}"
        }
        self.log.append(event)



    def propagate(self, request):
        """
        Propagates a deposit or withdrawal request to all other branches.

        Args:
            request (banks_pb2.TransactionRequest): The transaction to propagate.
        """
        for branch_id, branchStub in self.stubList:
            self.clock = self.clock + 1 #Lamport send
            out = banks_pb2.TransactionRequest(
                id=request.id, 
                amount=request.amount, 
                request_id=request.request_id, 
                clock=self.clock
            )

            interface = "propagate_deposit" if request.amount >= 0 else "propagate_withdraw"
            self.log_send(out, branch_id, interface)

            if (interface == "propagate_deposit"):
                branchStub.Propagate_Deposit(out)
            else:
                branchStub.Propagate_Withdraw(out)


    """
    Since the assignment spec requires a central handler, all RPC interface methods delegate to MsgDelivery.
    """

    def Deposit(self, request, context):
        return self.MsgDelivery(request, context)

    def Withdraw(self, request, context):
        return self.MsgDelivery(request, context)

    def Propagate_Deposit(self, request, context):
        return self.MsgDelivery(request, context)

    def Propagate_Withdraw(self, request, context):
        return self.MsgDelivery(request, context)
    

    def Get_Log(self, request, context):
        """
        Returns this branch's logged events.
        """
        response = banks_pb2.BranchLog()

        for entry in self.log:
            event = response.events.add()
            event.customer_request_id = entry["customer-request-id"]
            event.logical_clock = entry["logical_clock"]
            event.interface = entry["interface"]
            event.comment = entry["comment"]

        return response

    def MsgDelivery(self, request, context):
        """
        Central handler for all incoming transaction gRPC requests.

        Determines request type and processes accordingly.

        Args:
            request: The gRPC TransactionRequest message.
            context: The gRPC context object for the call.

        Returns:
            banks_pb2.TransactionResponse:
            The response message.
        """
        response = banks_pb2.TransactionResponse()

        if isinstance(request, banks_pb2.TransactionRequest):   #handle deposit or withdraw
            self.clock = max(self.clock, request.clock) + 1 #Lamport recieve
            
            self.log_receipt(request)   #log receipt

            if (request.amount + self.balance >= 0): #sufficient funds

                self.balance += request.amount  #update local balance

                if (request.id == self.id): #propagate customer requests
                    self.propagate(request)
    
        return response


