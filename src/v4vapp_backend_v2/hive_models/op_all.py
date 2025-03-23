from typing import Union

from .op_account_witness_vote import AccountWitnessVote
from .op_producer_reward import ProducerReward
from .op_transfer import Transfer

OpAny = Union[Transfer, ProducerReward, AccountWitnessVote]
