from v4vapp_backend_v2.hive_models.op_models import (
    HiveOpTypes,
    MarketOpTypes,
    RealOpsLoopTypes,
    TransferOpTypes,
    VirtualOpTypes,
    WitnessOpTypes,
    create_master_enum,
)


def test_create_master_enum():
    create_master_enum(TransferOpTypes, MarketOpTypes) == HiveOpTypes
    assert "fill_order" in HiveOpTypes
    assert "fill_order" in MarketOpTypes
    assert "fill_order" not in TransferOpTypes
    assert list(HiveOpTypes) == list(TransferOpTypes) + list(MarketOpTypes)
    assert VirtualOpTypes.PRODUCER_REWARD in VirtualOpTypes
