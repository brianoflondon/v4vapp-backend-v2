from enum import StrEnum, auto


class Events(StrEnum):
    # LND events
    LND_INVOICE = auto()
    LND_PAYMENT = auto()
    LND_INVOICE_COMPLETED = auto()
    LND_PAYMENT_COMPLETED = auto()

    # HTLC events
    HTLC_EVENT = auto()

    # Hive events
    HIVE_TRANSFER = auto()
