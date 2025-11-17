from pathlib import Path
from pprint import pprint

import pytest

from v4vapp_backend_v2.witness_monitor.witness_events import (
    call_hive_api,
    check_witness_heartbeat,
    send_kuma_heartbeat,
    update_witness_properties_switch_machine,
)


@pytest.fixture(autouse=True)
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


async def test_call_hive_api():
    test_rpc_nodes = ["https://api.hive.blog", "https://rpc.podping.org"]

    for rpc_node in test_rpc_nodes:
        result, execution_time = await call_hive_api(rpc_node, "example_machine")
        assert result is not None
        pprint(result)
        print(f"Execution time for {rpc_node}: {execution_time:.3f} seconds")


async def test_send_kuma_heartbeat():
    await send_kuma_heartbeat(witness="blocktrades", status="up", ping=0.23)


async def test_check_witness_heartbeat():
    await check_witness_heartbeat(
        witness="brianoflondon",
    )


async def test_update_witness_properties_switch_machine():
    witness_name = "brianoflondon"
    await update_witness_properties_switch_machine(
        witness_name=witness_name, machine_name="bol-2", nobroadcast=True
    )
    await update_witness_properties_switch_machine(
        witness_name=witness_name, machine_name="bol-1", nobroadcast=True
    )
    await update_witness_properties_switch_machine(
        witness_name="", machine_name="bol-1", nobroadcast=True
    )
