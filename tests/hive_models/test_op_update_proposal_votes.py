from pathlib import Path

import pytest

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive.voting_power import VotingPower
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


def test_op_update_proposal_votes(mocker):
    def fake_voting_power(voter: str, proposal: int = 0):
        voting_power = VotingPower()
        voting_power.voter = voter
        voting_power.proposal = proposal
        voting_power.proposal_total_votes = 1_000.0
        voting_power.vesting_power = 250.0
        voting_power.delegated_vesting_power = 0.0
        voting_power.received_vesting_power = 0.0
        voting_power.vote_value = 250.0
        voting_power.proxy_value = 0.0
        voting_power.prop_percent = 25.0
        voting_power.total_value = 250.0
        voting_power.total_percent = 12.5
        return voting_power

    mock_redis = mocker.Mock()
    mock_redis.get.return_value = None
    mock_redis.setex.return_value = None
    mocker.patch(
        "v4vapp_backend_v2.hive_models.op_update_proposal_votes.VotingPower",
        side_effect=fake_voting_power,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.op_update_proposal_votes.InternalConfig.redis_decoded",
        mock_redis,
        create=True,
    )

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
