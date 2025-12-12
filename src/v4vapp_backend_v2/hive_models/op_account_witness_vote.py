from dataclasses import asdict

from pydantic import Field

from v4vapp_backend_v2.hive.voting_power import VoterDetails, VotingPower
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType

from .op_base import OpBase


class AccountWitnessVote(OpBase):
    """
    Represents an account's witness vote operation in the Hive blockchain.

    Attributes:
        account (str): The name of the account performing the vote.
        approve (bool): Indicates whether the vote is to approve (True)
            or unapprove (False) the witness.
        voter_details (VoterDetails | None): Optional details about the voter,
            such as voting power and total value.
        witness (str): The name of the witness being voted for or unvoted.

    Methods:
        __init__(**data):
            Initializes the AccountWitnessVote instance with the provided data.

        get_voter_details():
            Retrieves and sets the voter details, including voting power and total value,
            for the account.

        log_str:
            A property that generates a human-readable log string summarizing the vote operation.
    """

    account: AccNameType = Field(description="Voting account")
    approve: bool
    voter_details: VoterDetails | None = None
    witness: str

    def __init__(self, **data):
        super().__init__(**data)

    def get_voter_details(self):
        """
        Retrieves and sets the voter details for the account.

        This method calculates the voting power of the account using the `VotingPower` class
        and validates the resulting data against the `VoterDetails` model. The validated
        voter details are then assigned to the `voter_details` attribute of the instance.

        Attributes:
            voter_details (VoterDetails): The validated voter details for the account.

        Raises:
            ValidationError: If the data from `VotingPower` does not conform to the
                             `VoterDetails` model schema.
        """
        voter_power = VotingPower(self.account)
        self.voter_details = VoterDetails.model_validate(asdict(voter_power))

    @property
    def log_common(self):
        """
        Generates a common log string for the operation.

        This method provides a common log string format that includes the block number
        and the account name.

        Returns:
            str: The common log string.
        """
        voted_for = "voted for" if self.approve else "unvoted"
        if self.voter_details:
            total_value = self.voter_details.total_value
        else:
            total_value = 0
        return f"👁️ {self.account} {voted_for} {self.witness} with {total_value:,.0f} HP"

    @property
    def log_str(self) -> str:
        return f"{self.log_common} {self.link} {self.short_id}"

    @property
    def notification_str(self) -> str:
        return f"{self.log_common} {self.markdown_link} {self.account.markdown_link}"


# Example usage
# data = {
#     "_id": "5eaa46458b2fa7b776eaaeb6c1c437027777ae8e",
#     "op_in_trx": 0,
#     "trx_id": "5eaa46458b2fa7b776eaaeb6c1c437027777ae8e",
#     "account": "ladyaryastark",
#     "approve": True,
#     "block_num": 94188656,
#     "timestamp": {"$date": "2025-03-16T14:55:45.000Z"},
#     "trx_num": 16,
#     "type": "account_witness_vote",
#     "vote_value": 241.55820571089652,
#     "voter_details": {
#         "voter": "ladyaryastark",
#         "proposal": 0,
#         "proposal_total_votes": 36456484.28235988,
#         "vesting_power": 241.55820571089652,
#         "delegated_vesting_power": 27.022683551570367,
#         "received_vesting_power": 44.024000911248805,
#         "vote_value": 241.55820571089652,
#         "proxy_value": 0,
#         "prop_percent": 0.0006625932545771528,
#         "total_value": 241.55820571089652,
#         "total_percent": 0.0006625932545771528,
#     },
#     "witness": "enginewitty",
# }
