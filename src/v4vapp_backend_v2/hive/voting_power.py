import logging
from dataclasses import dataclass

from nectar.account import Account
from nectar.hive import Hive
from pydantic import BaseModel

from v4vapp_backend_v2.hive.hive_extras import get_hive_client


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


@dataclass
class VotingPower:
    """
    A class to represent the voting power of a voter in a proposal.

    Attributes:
    ----------
    voter : str
        The name of the voter (default is "_total").
    proposal : int
        The ID of the proposal (default is 0).
    proposal_total_votes : float
        The total votes for the proposal (default is 0.0).
    vesting_power : float
        The vesting power of the voter (default is 0.0).
    delegated_vesting_power : float
        The delegated vesting power of the voter (default is 0.0).
    received_vesting_power : float
        The received vesting power of the voter (default is 0.0).
    vote_value : float
        The vote value of the voter (default is 0.0).
    proxy_value : float
        The proxy value of the voter (default is 0.0).
    prop_percent : float
        The percentage of the proposal votes (default is 0.0).
    total_value : float
        The total value of the voter (default is 0.0).
    total_percent : float
        The total percentage of the voter (default is 0.0).

    Methods:
    -------
    set_up(voter: str = "", hive: Hive = None, proposal: int = 0) -> None:
        Sets up the voting power for a given voter and proposal.

    __iadd__(self, other):
        Adds the voting power of another VotingPower instance to this instance.

    __str__() -> str:
        Returns a string representation of the voting power.
    """

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

    def __init__(self, voter: str = "", hive: Hive | None = None, proposal: int = 0):
        super().__init__()
        if voter:
            self.set_up(voter, hive, proposal)
        pass

    def set_up(
        self,
        voter: str = "",
        hive: Hive | None = None,
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
        self.delegated_vesting_power = hive.vests_to_token_power(acc["delegated_vesting_shares"])
        self.received_vesting_power = hive.vests_to_token_power(acc["received_vesting_shares"])
        self.proxy_value = hive.vests_to_token_power(acc["proxied_vsf_votes"][0] / 1000000)
        self.vote_value = self.vesting_power
        self.total_value = self.vote_value + self.proxy_value
        if not hive.rpc:
            raise Exception("No RPC nodes available to check proposal votes")
        # Proposal 233 is the Stabilizer proposal until May 2023
        # Change this to proposal 0 to get return proposal
        rpc_node_count = len(hive.rpc.nodes)
        proposals = []
        while rpc_node_count > 0:
            try:
                proposals = hive.rpc.find_proposals([proposal, 0])
                break
            except Exception as ex:
                logging.error(
                    f"Problem checking proposal votes {ex} {hive.rpc.url} no_preview",
                    exc_info=True,
                )
                rpc_node_count -= 1
                hive.rpc.next()
            finally:
                if rpc_node_count == 0:
                    raise Exception("No RPC nodes available to check proposal votes")
        if not proposals or len(proposals) < 2:
            raise Exception("No proposals found")
        return_prop = float(proposals[1]["total_votes"]) / 1e6
        return_prop = hive.vests_to_token_power(return_prop)
        this_prop = float(proposals[0]["total_votes"]) / 1e6
        self.proposal_total_votes = hive.vests_to_token_power(this_prop)
        try:
            # What percentage of the proposal total votes is this voter
            self.prop_percent = (self.total_value / self.proposal_total_votes) * 100
            # What percentage of the votes up to the return proposal is this vote
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


"""



hive-monitor-1     | 2025-04-22T12:17:37+0000.032 ERROR    voting_power              110 : Problem checking proposal votes
hive-monitor-1     | 2025-04-22T12:17:37+0000.033 ERROR    voting_power              111 : Unexpected response format: {'message': 'Internal Server Error'}
hive-monitor-1     | Traceback (most recent call last):
hive-monitor-1     |   File "/app/.venv/lib/python3.12/site-packages/nectarapi/noderpc.py", line 63, in rpcexec
hive-monitor-1     |     reply = super(NodeRPC, self).rpcexec(payload)
hive-monitor-1     |             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
hive-monitor-1     |   File "/app/.venv/lib/python3.12/site-packages/nectarapi/graphenerpc.py", line 525, in rpcexec
hive-monitor-1     |     raise RPCError(f"Unexpected response format: {ret}")
hive-monitor-1     | nectarapi.exceptions.RPCError: Unexpected response format: {'message': 'Internal Server Error'}
hive-monitor-1     |
hive-monitor-1     | During handling of the above exception, another exception occurred:
hive-monitor-1     |
hive-monitor-1     | Traceback (most recent call last):
hive-monitor-1     |   File "/app/src/v4vapp_backend_v2/hive/voting_power.py", line 108, in set_up
hive-monitor-1     |     proposals = hive.rpc.find_proposals([proposal, 0])
hive-monitor-1     |                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
hive-monitor-1     |   File "/app/.venv/lib/python3.12/site-packages/nectarapi/graphenerpc.py", line 558, in method
hive-monitor-1     |     r = self.rpcexec(query)
hive-monitor-1     |         ^^^^^^^^^^^^^^^^^^^
hive-monitor-1     |   File "/app/.venv/lib/python3.12/site-packages/nectarapi/noderpc.py", line 89, in rpcexec
hive-monitor-1     |     doRetry = self._check_error_message(e, self.error_cnt_call)
hive-monitor-1     |               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
hive-monitor-1     |   File "/app/.venv/lib/python3.12/site-packages/nectarapi/noderpc.py", line 183, in _check_error_message
hive-monitor-1     |     raise exceptions.UnhandledRPCError(msg)
hive-monitor-1     | nectarapi.exceptions.UnhandledRPCError: Unexpected response format: {'message': 'Internal Server Error'}


"""
