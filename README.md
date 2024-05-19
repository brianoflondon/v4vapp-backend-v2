# v4vapp-backend-v2
Back End for V4V.app Public Version 2


## Step 1: gPRC connection to LND

First step is following these instructions:

https://github.com/lightningnetwork/lnd/blob/master/docs/grpc/python.md

Which immideately fails because

`╰─± poetry add grpcio grpcio-tools googleapis-common-protos mypy-protobuf`

Fails with a version conflict.

`poetry add grpcio grpcio-tools@1.62.0 googleapis-common-protos@1.62.0 mypy-protobuf`

Works.


## Connecting to Umbrel at home.

This was the magic needed to connect to an IP address but actually connect to a host name which is in the tls certificate.

`channel = grpc.secure_channel("10.0.0.5:10009", creds, options=(('grpc.ssl_target_name_override', 'umbrel.local',),))`