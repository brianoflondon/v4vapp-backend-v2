import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.helpers.pub_key_alias import update_payment_route_with_alias
from v4vapp_backend_v2.models.payment_models import ListPaymentsResponse


def read_list_payments_raw(file_path: str) -> lnrpc.ListPaymentsResponse:
    with open(file_path, "rb") as file:
        return lnrpc.ListPaymentsResponse.FromString(file.read())


# TODO: #13 this needs to be able to use the database and lnd client
def test_route_in_payments():
    lnrpc_list_payments = read_list_payments_raw(
        "tests/data/lnd_lists/list_payments_raw.bin"
    )
    assert lnrpc_list_payments
    assert isinstance(lnrpc_list_payments, lnrpc.ListPaymentsResponse)
    list_payment_response = ListPaymentsResponse(lnrpc_list_payments)

    for payment in list_payment_response.payments:
        try:
            print(payment.destination_pub_keys)
            print(payment.destination)

        except Exception as e:
            print(e)
            assert False
