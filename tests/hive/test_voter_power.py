from dataclasses import asdict
from v4vapp_backend_v2.helpers.voting_power import VotingPower


def test_voting_power():
    vp = VotingPower()
    vp.set_up("brianoflondon", proposal=303)
    assert vp.voter == "brianoflondon"
    assert vp.proposal == 303
    assert vp.proposal_total_votes > 0
    assert vp.vesting_power > 0
    assert vp.delegated_vesting_power >= 0
    assert asdict(vp)['voter'] == "brianoflondon"
    
