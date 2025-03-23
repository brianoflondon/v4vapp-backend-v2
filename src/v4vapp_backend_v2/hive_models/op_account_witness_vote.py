from dataclasses import asdict
from datetime import datetime

from pydantic import BaseModel

from v4vapp_backend_v2.hive.voting_power import VotingPower

from .op_base import OpBase


class VoterDetails(BaseModel):
    voter: str
    proposal: int
    proposal_total_votes: float
    vesting_power: float
    delegated_vesting_power: float
    received_vesting_power: float
    vote_value: float
    proxy_value: float
    prop_percent: float
    total_value: float
    total_percent: float


class AccountWitnessVote(OpBase):
    op_in_trx: int
    trx_id: str
    account: str
    approve: bool
    block_num: int
    timestamp: datetime
    trx_num: int
    type: str
    voter_details: VoterDetails | None = None
    witness: str

    def __init__(self, **data):
        super().__init__(**data)

    def get_voter_details(self):
        voter_power = VotingPower(self.account)
        self.voter_details = VoterDetails.model_validate(asdict(voter_power))

    @property
    def log_str(self) -> str:
        voted_for = "voted for" if self.approve else "unvoted"
        if self.voter_details:
            total_value = self.voter_details.total_value
        else:
            total_value = 0
        log_str = (
            f"👁️ {self.account} "
            f"{voted_for} {self.witness} "
            f"with {total_value:,.0f} HP"
        )
        return log_str


# Example usage
data = {
    "_id": "5eaa46458b2fa7b776eaaeb6c1c437027777ae8e",
    "op_in_trx": 0,
    "trx_id": "5eaa46458b2fa7b776eaaeb6c1c437027777ae8e",
    "account": "ladyaryastark",
    "approve": True,
    "block_num": 94188656,
    "timestamp": {"$date": "2025-03-16T14:55:45.000Z"},
    "trx_num": 16,
    "type": "account_witness_vote",
    "vote_value": 241.55820571089652,
    "voter_details": {
        "voter": "ladyaryastark",
        "proposal": 0,
        "proposal_total_votes": 36456484.28235988,
        "vesting_power": 241.55820571089652,
        "delegated_vesting_power": 27.022683551570367,
        "received_vesting_power": 44.024000911248805,
        "vote_value": 241.55820571089652,
        "proxy_value": 0,
        "prop_percent": 0.0006625932545771528,
        "total_value": 241.55820571089652,
        "total_percent": 0.0006625932545771528,
    },
    "witness": "enginewitty",
}
