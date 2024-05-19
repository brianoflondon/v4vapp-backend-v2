import codecs
import os

import grpc
import lightning_pb2 as ln
import lightning_pb2_grpc as lnrpc

if __name__ == "__main__":
    # Create a channel to the server
    # Due to updated ECDSA generated tls.cert we need to let gprc know that
    # we need to use that cipher suite otherwise there will be a handshake
    # error when we communicate with the lnd rpc server.
    os.environ["GRPC_SSL_CIPHER_SUITES"] = "HIGH+ECDSA"

    # Lnd cert is at ~/.lnd/tls.cert on Linux and
    # ~/Library/Application Support/Lnd/tls.cert on Mac

    voltage = False
    if voltage:
        # Voltage working:
        cert = open(os.path.expanduser(".certs/tls-voltage.cert"), "rb").read()
        creds = grpc.ssl_channel_credentials(cert)
        with open(os.path.expanduser(".certs/readonly.macaroon"), "rb") as f:
            macaroon_bytes = f.read()
        macaroon = codecs.encode(macaroon_bytes, "hex")
        channel = grpc.secure_channel("v4vapp.m.voltageapp.io:10009", creds)

    else:
        # Local working:
        cert = open(os.path.expanduser(".certs/tls.cert"), "rb").read()
        creds = grpc.ssl_channel_credentials(cert)
        with open(os.path.expanduser(".certs/umbrel-admin.macaroon"), "rb") as f:
            macaroon_bytes = f.read()
        macaroon = codecs.encode(macaroon_bytes, "hex")
        # channel = grpc.secure_channel("umbrel.local:10009", creds)

        channel = grpc.secure_channel("10.0.0.5:10009", creds, options=(('grpc.ssl_target_name_override', 'umbrel.local',),))

    stub = lnrpc.LightningStub(channel)

    # Retrieve and display the wallet balance
    response = stub.WalletBalance(
        ln.WalletBalanceRequest(), metadata=[("macaroon", macaroon)]
    )
    print(response.total_balance)

    request = ln.InvoiceSubscription()
    for invoice in stub.SubscribeInvoices(request, metadata=[("macaroon", macaroon)]):
        print(invoice)
