from dataclasses import asdict
from pathlib import Path

import pytest

from v4vapp_backend_v2.hive.voting_power import VotingPower


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


# TODO: #83 Simuulate errors in fetching the proposal votes
def test_voting_power():
    vp = VotingPower()
    vp.set_up("blocktrades", proposal=303)
    assert vp.voter == "blocktrades"
    assert vp.proposal == 303
    assert vp.proposal_total_votes > 0
    assert vp.vesting_power > 0
    assert vp.delegated_vesting_power >= 0
    assert asdict(vp)["voter"] == "blocktrades"


def test_voting_power_init():
    vp = VotingPower(voter="brianoflondon", proposal=303)
    assert vp.voter == "brianoflondon"
    assert vp.proposal == 303
    assert vp.proposal_total_votes > 0
    assert vp.vesting_power > 0
    assert vp.delegated_vesting_power > 0
    assert asdict(vp)["voter"] == "brianoflondon"
