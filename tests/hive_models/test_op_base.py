from random import choice
from secrets import token_hex

from v4vapp_backend_v2.hive_models.op_base import OpBase, OpRealm
from v4vapp_backend_v2.hive_models.real_virtual_ops import (
    HIVE_REAL_OPS,
    HIVE_VIRTUAL_OPS,
)


def test_log_extra_real():
    op_type = choice(list(HIVE_REAL_OPS.keys()))
    trx_id = token_hex(20)

    op_base = OpBase(trx_id=trx_id, type=op_type)

    assert op_base.log_extra == {
        "op_base": {
            "realm": OpRealm.REAL,
            "trx_id": trx_id,
            "type": op_type,
            "op_in_trx": 0,
        }
    }
    assert op_base.name() == "op_base"


def test_log_extra_virtual():
    op_type = choice(list(HIVE_VIRTUAL_OPS.keys()))
    trx_id = token_hex(20)

    op_base = OpBase(trx_id=trx_id, type=op_type, op_in_trx=0)

    assert op_base.log_extra == {
        "op_base": {
            "realm": OpRealm.VIRTUAL,
            "trx_id": trx_id,
            "type": op_type,
            "op_in_trx": 0,
        }
    }
    assert op_base.name() == "op_base"
