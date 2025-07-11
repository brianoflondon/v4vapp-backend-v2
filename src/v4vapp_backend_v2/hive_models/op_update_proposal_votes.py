import json
from dataclasses import asdict
from typing import List

from pydantic import Field

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis
from v4vapp_backend_v2.hive.voting_power import VoterDetails, VotingPower
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.op_base import OpBase


# TODO: this needs a complete rething for multiple prop ids
class UpdateProposalVotes(OpBase):
    """
    Class to handle the update of proposal votes in the Hive blockchain.
    """

    approve: bool = False
    extensions: List[str] = []
    proposal_ids: List[int] = []
    voter: AccNameType = Field("", description="The account name of the voter")
    prop_voter_details: dict[str, VoterDetails] = Field(
        default_factory=dict,
        description="Voter details for each proposal (dict key is str for mongodb)",
    )

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
        for prop_id in self.proposal_ids:
            cache_key = f"voter_details_{self.voter}_{prop_id}"
            try:
                with V4VAsyncRedis().sync_redis as redis_client:
                    cached_data = redis_client.get(cache_key)
                    if cached_data:
                        self.prop_voter_details[str(prop_id)] = VoterDetails.model_validate(
                            json.loads(cached_data)
                        )
                        continue
            except Exception as e:
                logger.info(f"Error getting cache for {cache_key}: {e}")
            voter_power = VotingPower(self.voter, proposal=prop_id)
            self.prop_voter_details[str(prop_id)] = VoterDetails.model_validate(
                asdict(voter_power)
            )
            try:
                with V4VAsyncRedis().sync_redis as redis_client:
                    cache_value = self.prop_voter_details[str(prop_id)].model_dump_json()
                    redis_client.setex(
                        cache_key,
                        time=300,
                        value=cache_value,
                    )
            except Exception as e:
                logger.info(f"Error setting cache for {cache_key}: {e}")

    def _log_common(self, mardown: bool = False) -> str:
        """
        Generates a common log string for the operation.

        This method provides a common log string format that includes the block number
        and the account name.

        Returns:
            str: The common log string.
        """
        voted_for = "✅voted" if self.approve else "❌unvoted"

        prop_id_sections = []
        for prop_id in self.proposal_ids:
            details = self.prop_voter_details.get(str(prop_id), None)
            if details:
                total_percent = details.total_percent
                prop_percent = details.prop_percent
                prop_id_sections.append(
                    f"{prop_id} {prop_percent:.2f}% ({total_percent:.1f}% to ret)"
                )

        prop_id_sections = ", ".join(prop_id_sections)

        voter_details = self.prop_voter_details.get(str(self.proposal_ids[0]), None)
        if voter_details:
            vote_value = f"{voter_details.vote_value:>9,.0f} HP"
        else:
            vote_value = "unknown"

        voter = f"{self.voter.markdown_link}" if mardown else f"{self.voter:<20}"
        return f"👁️ {voter} {vote_value} {voted_for:<8} {prop_id_sections}"

    @property
    def is_tracked(self):
        if any(prop_id in self.proposals_tracked for prop_id in self.proposal_ids):
            return True
        return False

    @property
    def log_str(self) -> str:
        return f"{self._log_common()} {self.link}"

    @property
    def notification_str(self) -> str:
        return f"{self._log_common(True)} {self.markdown_link} {self.voter.markdown_link}"
