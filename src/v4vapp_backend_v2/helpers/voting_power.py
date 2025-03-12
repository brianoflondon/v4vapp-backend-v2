import logging
from dataclasses import dataclass

from beem import Hive
from beem.account import Account
from v4vapp_backend_v2.helpers.hive_extras import get_hive_client

@dataclass
class VotingPower:
    voter: str = "_total"
    proposal: int = 0
    proposal_total_votes: float = 0.0
    vesting_power: float = 0.0
    delegated_vesting_power: float = 0.0
    received_vesting_power: float = 0.0
    vote_value: float = 0.0
    proxy_value: float = 0.0
    prop_percent: float = 0.0
    total_value: float = 0.0
    total_percent: float = 0.0

    def set_up(
        self,
        voter: str = "",
        hive: Hive = None,
        proposal: int = 0,
    ) -> None:
        if not voter:
            return
        if not hive:
            hive = get_hive_client()

        acc = Account(voter)
        self.proposal = proposal
        self.voter = acc.name
        self.vesting_power = hive.vests_to_token_power(acc["vesting_shares"])
        self.delegated_vesting_power = hive.vests_to_token_power(
            acc["delegated_vesting_shares"]
        )
        self.received_vesting_power = hive.vests_to_token_power(
            acc["received_vesting_shares"]
        )
        self.proxy_value = hive.vests_to_token_power(
            acc["proxied_vsf_votes"][0] / 1000000
        )
        self.vote_value = self.vesting_power
        self.total_value = self.vote_value + self.proxy_value
        # Proposal 233 is the Stabiliser proposal until May 2023
        # Change this to proposal 0 to get return proposal
        try:
            proposals = hive.rpc.find_proposals([proposal, 0])
        except Exception as ex:
            logging.error("Problem checking proposal votes")
            logging.exception(ex)
            return
        return_prop = float(proposals[1]["total_votes"]) / 1e6
        return_prop = hive.vests_to_token_power(return_prop)
        this_prop = float(proposals[0]["total_votes"]) / 1e6
        self.proposal_total_votes = hive.vests_to_token_power(this_prop)
        try:
            self.prop_percent = (self.total_value / self.proposal_total_votes) * 100
            self.total_percent = (self.total_value / return_prop) * 100
        except ZeroDivisionError:
            self.prop_percent = 0
            self.total_percent = 0

    def __iadd__(self, other):
        """Addition"""
        self.voter = "_total"
        if isinstance(other, VotingPower):
            self.proposal = other.proposal
            self.vesting_power += other.vesting_power
            self.delegated_vesting_power += other.delegated_vesting_power
            self.received_vesting_power += other.received_vesting_power
            self.vote_value += other.vote_value
            self.total_value += other.total_value
            self.proxy_value += other.proxy_value
            self.prop_percent += other.prop_percent
            self.total_percent += other.total_percent
        return self

    def __str__(self) -> str:
        return (
            f"{self.voter:>18} {self.vote_value:>11,.0f} "
            f"{self.proxy_value:>11,.0f} {self.total_value:>11,.0f} "
            f"{self.prop_percent:>4.1f} % {self.total_percent:>4.1f} %"
        )
