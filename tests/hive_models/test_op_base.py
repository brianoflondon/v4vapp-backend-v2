from v4vapp_backend_v2.hive_models.op_base import OpBase


def test_log_extra():
    op_base = OpBase()
    assert op_base.log_extra == {"op_base": {}}
    assert op_base.name() == "op_base"
