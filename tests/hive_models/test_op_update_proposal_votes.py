from pathlib import Path

import pytest

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.op_update_proposal_votes import UpdateProposalVotes


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


def test_op_update_proposal_votes():
    count = 0
    for hive_event in load_hive_events():
        if hive_event["type"] == "update_proposal_votes":
            update_proposal_votes = UpdateProposalVotes.model_validate(hive_event)
            assert update_proposal_votes.trx_id == hive_event["trx_id"]
            for proposal_id in hive_event["proposal_ids"]:
                count += 1
                update_proposal_votes.get_voter_details()
                assert (
                    update_proposal_votes.prop_voter_details[str(proposal_id)].voter
                    == hive_event["voter"]
                )
                print(update_proposal_votes.log_str)
                assert isinstance(update_proposal_votes.log_str, str)
                assert isinstance(update_proposal_votes.notification_str, str)
    print(f"count: {count}")
