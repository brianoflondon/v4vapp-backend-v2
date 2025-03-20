from enum import StrEnum, auto


class Events(StrEnum):
    # LND events
    LND_INVOICE = auto()
    LND_PAYMENT = auto()
    LND_INVOICE_COMPLETED = auto()
    LND_PAYMENT_COMPLETED = auto()

    # HTLC events
    HTLC_EVENT = auto()

    # Hive Transfer events
    HIVE_TRANSFER = auto()
    HIVE_TRANSFER_NOTIFY = auto()

    # Hive Witness Vote events
    HIVE_WITNESS_VOTE = auto()

    # Hive Market events
    HIVE_MARKET_FILL_ORDER = auto()
    HIVE_MARKET_LIMIT_ORDER_CREATE = auto()
    HIVE_MARKET_LIMIT_ORDER_CANCEL = auto()
