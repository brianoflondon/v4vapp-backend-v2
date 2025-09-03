from enum import StrEnum


class LedgerType(StrEnum):
    """
    Enumeration of ledger entry types for accounting transactions.
    value char length must be less than or equal to 10 chars

    Attributes:
        - UNSET: Default value for unset ledger type
        - CONV_H_L: Conversion from Hive to Lightning
        - CONTRA_H_L: Contra entry for Hive to Lightning conversion
        - CONV_L_H: Conversion from Lightning to Hive
        - CONTRA_L_H: Contra entry for Lightning to Hive conversion
        - HIVE_FEE: Fee applied to Hive transactions
        - LIGHTNING_FEE: Fee applied to Lightning transactions
        - LIGHTNING_CONTRA: Contra entry for Lightning transactions
        - LIGHTNING_OUT: Outgoing Lightning transaction
        - LIGHTNING_IN: Incoming Lightning transaction
        - HIVE_IN: Incoming Hive transaction
        - HIVE_OUT: Outgoing Hive transaction
    """

    UNSET = "unset"  # Default value for unset ledger type

    FUNDING = "funding"  # Funding from Owner to Treasury

    CONV_HIVE_TO_LIGHTNING = "h_conv_l"  # Conversion from Hive to Lightning
    CONV_LIGHTNING_TO_HIVE = "l_conv_h"  # Conversion from Lightning to Hive

    CONV_HIVE_TO_KEEPSATS = "h_conv_k"  # Conversion from Hive to Keepsats
    CONV_KEEPSATS_TO_HIVE = "k_conv_h"  # Conversion from Keepsats to Hive

    DEPOSIT_KEEPSATS = "deposit_k"  # Deposit into Keepsats account
    WITHDRAW_KEEPSATS = "withdraw_k"  # Withdrawal from Keepsats account
    DEPOSIT_HIVE = "deposit_h"  # Deposit into a customer's liability account
    WITHDRAW_HIVE = "withdraw_h"  # Withdrawal to a customer's liability account

    HOLD_KEEPSATS = "hold_k"  # Holding Keepsats in the account
    RELEASE_KEEPSATS = "release_k"  # Release Keepsats from the account

    CUSTOM_JSON_TRANSFER = "c_j_trans"  # Custom JSON transfer or notification
    CUSTOM_JSON_FEE = "c_j_fee"  # Custom JSON fee notification
    CUSTOM_JSON_NOTIFICATION = "cust_json"  # Custom JSON notification

    WITHDRAW_LIGHTNING = "withdraw_l"
    DEPOSIT_LIGHTNING = "deposit_l"

    LIGHTNING_EXTERNAL_SEND = "l_ext_out"  # Perhaps change to l_external_out
    LIGHTNING_EXTERNAL_IN = "l_ext_in"  # Lightning incoming transaction

    CONTRA_HIVE_TO_LIGHTNING = "h_contra_l"
    CONTRA_LIGHTNING_TO_HIVE = "l_contra_h"  # Contra entry for Lightning to Hive conversion

    CONSUME_CUSTOMER_KEEPSATS = "consume_k"  # Consume customer SATS for conversion

    CONTRA_HIVE_TO_KEEPSATS = "h_contra_k"  # Contra entry for Hive to Keepsats conversion
    CONTRA_KEEPSATS_TO_HIVE = "k_contra_h"  # Contra entry for Keepsats to Hive conversion
    RECLASSIFY_VSC_SATS = "r_vsc_sats"  # Reclassify VSC Liability (server) balance
    RECLASSIFY_VSC_HIVE = "r_vsc_hive"  # Reclassify VSC Liability (server) balance

    FEE_INCOME = "fee_inc"  # Fee income from Hive transactions
    FEE_EXPENSE = "fee_exp"  # Fee expense from Lightning transactions
    FEE_CHARGE = "fee_charge"  # Fee charges from a customer

    CUSTOMER_HIVE_IN = "cust_h_in"  # Customer deposit into Hive account
    CUSTOMER_HIVE_OUT = "cust_h_out"  # Customer withdrawal from Hive account

    SERVER_TO_TREASURY = "serv_to_t"  # Server to Treasury transfer
    TREASURY_TO_SERVER = "t_to_serv"  # Treasury to Server transfer
    FUNDING_TO_TREASURY = "fund_to_t"  # Funding to Treasury transfer
    TREASURY_TO_FUNDING = "t_to_fund"  # Treasury to Funding transfer
    TREASURY_TO_EXCHANGE = "t_to_exc"  # Treasury to Exchange transfer
    EXCHANGE_TO_TREASURY = "exc_to_t"  # Exchange to Treasury transfer
    LIMIT_ORDER_CREATE = "limit_or"
    FILL_ORDER_SELL = "fill_or_s"
    FILL_ORDER_BUY = "fill_or_b"

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
