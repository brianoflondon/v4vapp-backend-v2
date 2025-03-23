from typing import Union

from .op_producer_reward import ProducerReward
from .op_transfer import Transfer

OpAll = Union[Transfer]


class OpAny(Transfer, ProducerReward):
    pass
