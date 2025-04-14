from pathlib import Path
from random import choice
from secrets import token_hex

import httpx
import pytest

from v4vapp_backend_v2.hive_models.op_base import HiveExp, OpBase, get_hive_block_explorer_link
from v4vapp_backend_v2.hive_models.real_virtual_ops import HIVE_REAL_OPS, HIVE_VIRTUAL_OPS


@pytest.fixture(autouse=True)
def configure_and_reset_config(monkeypatch: pytest.MonkeyPatch):
    # Set up base config paths
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )

    # Reset the singleton instance before each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    # Reset the singleton instance after each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


def test_log_extra_real():
    op_type = choice(list(HIVE_REAL_OPS.keys()))
    trx_id = token_hex(20)

    op_base = OpBase(
        trx_id=trx_id,
        type=op_type,
        op_in_trx=1,
        block_num=94425724,
        trx_num=1,
        timestamp="2023-10-01T00:00:00",
    )
    assert op_base.log_extra.get("op_base")["trx_id"] == trx_id

    assert op_base.name() == "op_base"


def test_log_extra_virtual():
    op_type = choice(list(HIVE_VIRTUAL_OPS.keys()))
    trx_id = token_hex(20)

    op_base = OpBase(
        trx_id=trx_id,
        type=op_type,
        op_in_trx=1,
        block_num=94425724,
        trx_num=1,
        timestamp="2023-10-01T00:00:00",
    )
    assert op_base.log_extra.get("op_base")["trx_id"] == trx_id
    assert op_base.name() == "op_base"


def test_op_base_model_dump():
    op_type = choice(list(HIVE_VIRTUAL_OPS.keys()))
    trx_id = token_hex(20)

    op_base = OpBase(
        trx_id=trx_id,
        type=op_type,
        op_in_trx=1,
        block_num=94425724,
        trx_num=1,
        timestamp="2023-10-01T00:00:00",
    )

    print(op_base.model_dump())
    print(op_base.log_str)
    print(op_base.notification_str)
    print(op_base.logs)


# TODO: need far better testing of this
def test_get_hive_block_explorer_link():
    trx_id = "fd321bb9a7ac53ec1a7a04fcca0d0913a089ac2b"
    for block_explorer in HiveExp:
        link = get_hive_block_explorer_link(trx_id, block_explorer)
        try:
            response = httpx.get(link)
            assert response.status_code == 200
        except httpx.HTTPStatusError as e:
            print(f"{block_explorer.name}: {link} - {e}")
        print(f"{block_explorer.name}: {link}")
