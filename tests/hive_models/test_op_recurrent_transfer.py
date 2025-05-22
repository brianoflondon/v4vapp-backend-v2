from pathlib import Path

import pytest

from tests.load_data import load_hive_events
from tests.setup_quote import load_mock_last_quote
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.hive_models.op_all import OpAllRecurrent, OpAllTransfers, op_any_or_base
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.op_fill_recurrent_transfer import FillRecurrentTransfer
from v4vapp_backend_v2.hive_models.op_recurrent_transfer import RecurrentTransfer


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # No need to restore the original value, monkeypatch will handle it


@pytest.mark.asyncio
async def test_find_recurrent_transfers():
    all_recurrent_ops = [
        "recurrent_transfer",
        "fill_recurrent_transfer",
        "failed_recurrent_transfer",
    ]
    TrackedBaseModel.last_quote = load_mock_last_quote()
    OpBase.watch_users = ["spartano", "risingstar2"]
    for hive_event in load_hive_events():
        op = op_any_or_base(hive_event)
        if isinstance(op, OpAllTransfers):
            assert isinstance(op, OpAllTransfers)
            assert isinstance(op, OpBase)
            assert op.markdown_link
            if op.type in all_recurrent_ops:
                assert isinstance(op, OpAllTransfers)
                assert isinstance(op, OpAllRecurrent)
                assert isinstance(op, OpBase)
                assert op.markdown_link
                if op.type == "recurrent_transfer":
                    assert isinstance(op, RecurrentTransfer)
                if op.type == "fill_recurrent_transfer":
                    assert isinstance(op, FillRecurrentTransfer)
        if op.is_watched:
            if op.type in all_recurrent_ops:
                print(op.log_str)
