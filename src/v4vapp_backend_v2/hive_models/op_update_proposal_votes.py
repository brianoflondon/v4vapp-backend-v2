from dataclasses import asdict
from typing import List

from pydantic import Field

from v4vapp_backend_v2.hive.voting_power import VoterDetails, VotingPower
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.op_base import OpBase


class UpdateProposalVotes(OpBase):
    """
    Class to handle the update of proposal votes in the Hive blockchain.
    """

    approve: bool = False
    extensions: List[str] = []
    proposal_ids: List[int] = []
    voter: AccNameType = Field("", description="The account name of the voter")
    voter_details: VoterDetails | None = None

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
        voter_power = VotingPower(self.voter, proposal=self.proposal_ids[0])
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
        return (
            f"👁️ {self.block_num:,} {self.voter} "
            f"{voted_for} {self.proposal_ids} "
            f"with {total_value:,.0f} HP"
        )

    @property
    def log_str(self) -> str:
        return f"{self.log_common} {self.link}"

    @property
    def notification_str(self) -> str:
        return f"{self.log_common} {self.markdown_link} {self.voter.markdown_link}"
