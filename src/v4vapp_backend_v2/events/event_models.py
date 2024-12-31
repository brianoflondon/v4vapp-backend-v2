from enum import StrEnum, auto


class Events(StrEnum):
    # LND events
    LND_INVOICE = auto()
    LND_PAYMENT = auto()

    # HTLC events
    HTLC_EVENT = auto()
