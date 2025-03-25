from datetime import datetime
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

    op_base = OpBase(
        trx_id=trx_id, type=op_type, op_in_trx=0, block_num=94425724, trx_num=1
    )

    assert op_base.log_extra == {
        "op_base": {
            "realm": OpRealm.REAL,
            "trx_id": trx_id,
            "type": op_type,
            "op_in_trx": 0,
            "block_num": 94425724,
            "trx_num": 1,
        }
    }
    assert op_base.name() == "op_base"


def test_log_extra_virtual():
    op_type = choice(list(HIVE_VIRTUAL_OPS.keys()))
    trx_id = token_hex(20)

    op_base = OpBase(
        trx_id=trx_id, type=op_type, op_in_trx=0, block_num=94425724, trx_num=1
    )

    assert op_base.log_extra == {
        "op_base": {
            "realm": OpRealm.VIRTUAL,
            "trx_id": trx_id,
            "type": op_type,
            "op_in_trx": 0,
            "block_num": 94425724,
            "trx_num": 1,
        }
    }
    assert op_base.name() == "op_base"


def test_op_base_model_dump():
    op_type = choice(list(HIVE_VIRTUAL_OPS.keys()))
    trx_id = token_hex(20)

    op_base = OpBase(
        trx_id=trx_id, type=op_type, op_in_trx=0, block_num=94425724, trx_num=1
    )

    print(op_base.model_dump())
    print(op_base.log_str)
    print(op_base.notification_str)
    print(op_base.logs)
