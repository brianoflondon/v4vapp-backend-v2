from enum import StrEnum, auto
from typing import Tuple

from v4vapp_backend_v2.models.lnd_models import LNDInvoice

class Events(StrEnum):
    # LND events
    LND_INVOICE = auto()
    LND_INVOICE_CREATED = auto()
    LND_INVOICE_SETTLED = auto()
    LND_PAYMENT_SENT = auto()
    LND_PAYMENT_RECEIVED = auto()
    # HTLC events
    HTLC_EVENT = auto()
    HTLC_UNKNOWN = auto()
    HTLC_SEND = auto()
    HTLC_RECEIVE = auto()
    HTLC_FORWARD = auto()