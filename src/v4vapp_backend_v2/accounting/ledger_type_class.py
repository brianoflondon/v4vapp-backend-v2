from enum import StrEnum
from typing import Dict


class LedgerType(StrEnum):
    """
    Enumeration of ledger entry types for accounting transactions.
    value char length must be less than or equal to 10 chars


    """

    UNSET = "unset"  # Default value for unset ledger type

    FUNDING = "funding"  # Funding from Owner to Treasury

    ROUTING_FEE = "r_fee"  # Routing fee earned on HTLC forwards

    EXCHANGE_CONVERSION = "exc_conv"  # Conversion transaction on an external exchange
    EXCHANGE_FEES = "exc_fee"  # Fees paid to an external exchange

    CONV_CUSTOMER = "cust_conv"  # Customer conversion transaction

    CONV_HIVE_TO_KEEPSATS = "h_conv_k"  # Conversion from Hive to Keepsats
    CONV_KEEPSATS_TO_HIVE = "k_conv_h"  # Conversion from Keepsats to Hive

    # These two are deprecated; need new test data.
    WITHDRAW_HIVE = "withdraw_h"  # Withdrawal to a customer's liability account
    DEPOSIT_HIVE = "deposit_h"  # Deposit into a customer's liability account

    HOLD_KEEPSATS = "hold_k"  # Holding Keepsats in the account
    RELEASE_KEEPSATS = "release_k"  # Release Keepsats from the account

    CUSTOM_JSON_TRANSFER = "c_j_trans"  # Custom JSON transfer or notification
    CUSTOM_JSON_FEE = "c_j_fee"  # Custom JSON fee notification
    CUSTOM_JSON_FEE_REFUND = "c_j_fee_r"  # Custom JSON fee refund notification
    RECEIVE_LIGHTNING = "recv_l"  # Receive Custom JSON from external source

    WITHDRAW_LIGHTNING = "withdraw_l"
    DEPOSIT_LIGHTNING = "deposit_l"

    CONSUME_CUSTOMER_KEEPSATS = "consume_k"  # Consume customer SATS for conversion

    CONTRA_HIVE_TO_KEEPSATS = "h_contra_k"  # Contra entry for Hive to Keepsats conversion
    CONTRA_KEEPSATS_TO_HIVE = "k_contra_h"  # Contra entry for Keepsats to Hive conversion
    RECLASSIFY_VSC_SATS = "r_vsc_sats"  # Reclassify VSC Liability (server) balance
    RECLASSIFY_VSC_HIVE = "r_vsc_hive"  # Reclassify VSC Liability (server) balance

    FEE_INCOME = "fee_inc"  # Fee income from Hive transactions
    FEE_EXPENSE = "fee_exp"  # Fee expense from Lightning transactions

    CUSTOMER_HIVE_IN = "cust_h_in"  # Customer deposit into Hive account
    CUSTOMER_HIVE_OUT = "cust_h_out"  # Customer withdrawal from Hive account

    SERVER_TO_TREASURY = "serv_to_t"  # Server to Treasury transfer
    TREASURY_TO_SERVER = "t_to_serv"  # Treasury to Server transfer
    TREASURY_TO_FUNDING = "t_to_fund"  # Treasury to Funding transfer
    TREASURY_TO_EXCHANGE = "t_to_exc"  # Treasury to Exchange transfer
    EXCHANGE_TO_TREASURY = "exc_to_t"  # Exchange to Treasury transfer
    LIMIT_ORDER_CREATE = "limit_or"
    FILL_ORDER_SELL = "fill_or_s"
    FILL_ORDER_BUY = "fill_or_b"
    FILL_ORDER_NET = "fill_or_n"

    @property
    def printout(self) -> str:
        """Returns the string representation of the ledger type.

        This property is used to provide a human-readable format of the ledger type,
        which can be useful for logging or displaying in user interfaces.

        Returns:
            str: The string representation of the ledger type.
        """
        ans = " ".join(word.capitalize() for word in self.name.split("_"))
        ans = f"{ans} ({self.name} {self.value})"
        return ans

    @property
    def capitalized(self) -> str:
        """Returns the capitalized string representation of the ledger type.

        This property is used to provide a human-readable format of the ledger type,
        which can be useful for logging or displaying in user interfaces.

        Returns:
            str: The capitalized string representation of the ledger type.
        """
        ans = " ".join(word.capitalize() for word in self.name.split("_"))
        return ans


"""
These two classes are used in the accounting classes to provide icons and string representations
for different ledger types.

These surface in the frontend UI and keep users from seeing the raw ledger type values.

Class: AccountBalanceLine - src/v4vapp_backend_v2/accounting/accounting_classes.py

Attributes:
    - LedgerTypeIcon: A dictionary mapping LedgerType to its corresponding icon (str).
    - LedgerTypeStr: A dictionary mapping LedgerType to its corresponding string representation (str).


"""
LedgerTypeIcon: Dict[LedgerType, str] = {
    # LedgerType.DEPOSIT_HIVE: "📥",  # Deposit into a customer's liability account
    # LedgerType.WITHDRAW_HIVE: "📤",  # Withdrawal to a customer's liability account
    LedgerType.CUSTOMER_HIVE_OUT: "📤",  # Customer withdrawal from Hive account
    LedgerType.CUSTOMER_HIVE_IN: "📥",  # Customer deposit into Hive account
    LedgerType.CUSTOM_JSON_TRANSFER: "🔄",  # Custom JSON transfer or notification
    LedgerType.FEE_INCOME: "💵",  # Fee income from Hive transactions
    LedgerType.CONSUME_CUSTOMER_KEEPSATS: "🍽️",  # Consume customer SATS for conversion
    LedgerType.HOLD_KEEPSATS: "⏳",  # Holding Keepsats in the account
    LedgerType.CUSTOM_JSON_FEE: "💵",  # Custom JSON fee notification
    LedgerType.CUSTOM_JSON_FEE_REFUND: "↩️",  # Custom JSON fee refund notification
    LedgerType.RELEASE_KEEPSATS: "🚀",  # Release Keepsats from the account
    LedgerType.WITHDRAW_LIGHTNING: "⚡",  # Withdrawal to send to lightning invoice
    LedgerType.RECEIVE_LIGHTNING: "⚡",  # Receive Lightning payment
    LedgerType.CONV_CUSTOMER: "🔄",  # Conversion from Keepsats to Hive
}

LedgerTypeStr: Dict[LedgerType, str] = {
    LedgerType.FEE_INCOME: "Fee",  # Fee income from Hive transactions
    LedgerType.CUSTOM_JSON_FEE: "Fee",  # Custom JSON fee notification
    LedgerType.CUSTOM_JSON_FEE_REFUND: "Fee Refund",  # Custom JSON fee refund notification
    LedgerType.CONV_CUSTOMER: "Conversion",  # Conversion to/from Keepsats to Hive
    LedgerType.CUSTOMER_HIVE_OUT: "Withdraw",  # Customer withdrawal from Hive account
    LedgerType.CUSTOMER_HIVE_IN: "Deposit",  # Customer deposit into Hive
    LedgerType.WITHDRAW_LIGHTNING: "Send",  # Withdrawal to send to lightning invoice
    LedgerType.RECEIVE_LIGHTNING: "Receive",  # Receive Lightning payment
}


# IconStr = str
# TypeStr = str
# DescriptionStr = str

# LedgerTypeDetails: Dict[LedgerType, Tuple[IconStr, TypeStr, DescriptionStr]] = {
#     LedgerType.FEE_INCOME: ("💵", "Fee", "Fee income from Hive transactions"),
#     LedgerType.CUSTOM_JSON_FEE: ("💵", "Fee", "Custom JSON fee notification"),
#     LedgerType.CONV_CUSTOMER: ("🔄", "Conversion", "Conversion to/from Keepsats to Hive"),
#     LedgerType.CUSTOMER_HIVE_OUT: ("📤", "Withdraw", "Customer withdrawal from Hive account"),
#     LedgerType.CUSTOMER_HIVE_IN: ("📥", "Deposit", "Customer deposit into Hive account"),
# }
