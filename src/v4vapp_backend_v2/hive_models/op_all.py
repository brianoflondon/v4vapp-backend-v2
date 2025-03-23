from typing import Union

from .op_producer_reward import ProducerReward
from .op_transfer import TransferEnhanced

OpAll = Union[TransferEnhanced]


class OpAny(TransferEnhanced, ProducerReward):
    pass
