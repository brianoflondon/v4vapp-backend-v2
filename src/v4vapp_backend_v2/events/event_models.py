from enum import StrEnum, auto
from typing import Tuple

from v4vapp_backend_v2.models.lnd_models import LNDInvoice

class Events(StrEnum):
    # Event types
    LND_INVOICE_CREATED = auto()
    LND_PAYMENT_SENT = auto()
    LND_PAYMENT_RECEIVED = auto()


