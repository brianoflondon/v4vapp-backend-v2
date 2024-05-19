from google.protobuf.json_format import MessageToDict
from pydantic import ValidationError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
from v4vapp_backend_v2.lnd_grpc.connect import lnd_stub
from v4vapp_backend_v2.lnd_grpc.lnd_models import LNDInvoice

if __name__ == "__main__":
    stub = lnd_stub()
    response = stub.WalletBalance(ln.WalletBalanceRequest())
    print(response)

    response_inv = stub.ListInvoices(
        ln.ListInvoiceRequest(
            pending_only=False, reversed=True, index_offset=0, num_max_invoices=10
        )
    )
    for inv in response_inv.invoices:
        inv_dict = MessageToDict(inv, preserving_proto_field_name=True)
        try:
            invoice = LNDInvoice.model_validate(inv_dict)
            print(f"✅ Valid invoice {invoice.add_index}")
        except ValidationError as e:
            print(e)
            print(f"❌ Invalid invoice {inv.add_index}")

    # print(response)

    response_payment = stub.ListPayments(
        ln.ListPaymentsRequest(reversed=True, index_offset=0, max_payments=1)
    )

    for pay in response_payment.payments:
        print(pay)
        print(MessageToDict(pay, preserving_proto_field_name=True))
        print()

    request_sub = ln.InvoiceSubscription()
    for inv in stub.SubscribeInvoices(request_sub):
        inv_dict = MessageToDict(inv, preserving_proto_field_name=True)
        try:
            invoice = LNDInvoice.model_validate(inv_dict)
            print(f"✅ Valid invoice {invoice.add_index}")
        except ValidationError as e:
            print(e)
            print(f"❌ Invalid invoice {inv.add_index}")
