from v4vapp_backend_v2.lnd_grpc import connect





def test_lnd_stub():
    stub = connect.lnd_stub()
    assert stub is not None


