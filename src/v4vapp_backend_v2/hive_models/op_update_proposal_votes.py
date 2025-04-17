import json
from dataclasses import asdict
from typing import List

from pydantic import Field

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis
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
    prop_voter_details: dict[int, VoterDetails] = Field(
        default_factory=dict, description="Voter details for each proposal"
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
                        self.prop_voter_details[prop_id] = VoterDetails.model_validate(
                            json.loads(cached_data)
                        )
                        continue
            except Exception as e:
                logger.info(f"Error getting cache for {cache_key}: {e}")
            voter_power = VotingPower(self.voter, proposal=prop_id)
            self.prop_voter_details[prop_id] = VoterDetails.model_validate(asdict(voter_power))
            try:
                with V4VAsyncRedis().sync_redis as redis_client:
                    cache_value = self.prop_voter_details[prop_id].model_dump_json()
                    redis_client.setex(
                        cache_key,
                        time=300,
                        value=cache_value,
                    )
            except Exception as e:
                logger.info(f"Error setting cache for {cache_key}: {e}")

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
        if self.prop_voter_details:
            total_value = sum(detail.total_value for detail in self.prop_voter_details.values())
            total_percent = sum(
                detail.total_percent for detail in self.prop_voter_details.values()
            )
            total_prop_percent = sum(
                detail.prop_percent for detail in self.prop_voter_details.values()
            )
        else:
            total_value = 0
        return (
            f"👁️ {self.block_num:,} {self.voter} "
            f"{voted_for} {self.proposal_ids} "
            f"with {total_value:,.0f} HP "
            f"{total_percent:,.2f} % ({total_prop_percent:,.2f})"
        )

    @property
    def is_tracked(self):
        if any(prop_id in self.proposals_tracked for prop_id in self.proposal_ids):
            return True
        return False

    @property
    def log_str(self) -> str:
        return f"{self.log_common} {self.link}"

    @property
    def notification_str(self) -> str:
        return f"{self.log_common} {self.markdown_link} {self.voter.markdown_link}"
