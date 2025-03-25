from typing import Union

from .op_account_witness_vote import AccountWitnessVote
from .op_base import OpBase
from .op_custom_json import CustomJson
from .op_fill_order import FillOrder
from .op_limit_order_create import LimitOrderCreate
from .op_producer_reward import ProducerReward
from .op_transfer import Transfer

OpMarket = Union[FillOrder, LimitOrderCreate]

OpAny = Union[Transfer, ProducerReward, AccountWitnessVote, CustomJson, OpBase]
OpVirtual = Union[ProducerReward, FillOrder]
OpRealOpsLoop = Union[OpAny, OpMarket]
