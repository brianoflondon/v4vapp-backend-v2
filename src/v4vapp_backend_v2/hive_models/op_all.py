from typing import Union

from .op_base import OpBase
from .op_account_witness_vote import AccountWitnessVote
from .op_fill_order import FillOrder
from .op_limit_order_create import LimitOrderCreate
from .op_producer_reward import ProducerReward
from .op_transfer import Transfer
from .op_custom_json import CustomJson

OpMarket = Union[FillOrder, LimitOrderCreate]

OpAny = Union[Transfer, ProducerReward, AccountWitnessVote]

# OpMarket = Union[FillOrder, LimitOrderCreate, LimitOrderCancel]
# OpRealOpsLoop = Union[Transfer, ProducerReward, FillOrder, LimitOrderCreate, LimitOrderCancel, AccountWitnessVote]
