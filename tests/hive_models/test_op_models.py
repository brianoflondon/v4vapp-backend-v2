from v4vapp_backend_v2.hive_models.op_types_enums import (
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


def test_other_ops():
    assert "limit_order_create" in MarketOpTypes
    assert "limit_order_cancel" in MarketOpTypes
    assert "transfer" in TransferOpTypes
    assert "recurrent_transfer" in RealOpsLoopTypes
    assert "producer_reward" in WitnessOpTypes


def test_list_all_real_ops():
    for item in RealOpsLoopTypes:
        print(item)
        assert isinstance(item, str)
