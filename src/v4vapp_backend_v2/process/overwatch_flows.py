"""
Overwatch flow definitions.

Each flow definition describes the expected stages (ledger entries and operations)
for a particular transaction type. These are used by FlowInstance to determine
whether a transaction flow completed successfully.
"""

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.process_overwatch import FlowDefinition, FlowStage

# ---------------------------------------------------------------------------
# Hive-to-Keepsats conversion flow
# ---------------------------------------------------------------------------
# Flow: User sends HIVE to the server account with memo "Deposit to #SATS".
# The system converts the HIVE to sats stored on the system (keepsats),
# deducting a fee, and returns change (dust) back to the user.
#
# Primary events (same short_id as trigger):
#   1. transfer op (trigger)
#   2. cust_h_in ledger - Customer deposits HIVE
#   3. hold_k ledger - Fee sats held temporarily
#   4. h_conv_k ledger - Conversion from HIVE to sats
#   5. h_contra_k ledger - Contra/offset entry
#   6. cust_conv ledger - Net sats credited to customer
#   7. release_k ledger - Fee sats released
#
# Reply events (fee notification - different short_id, linked via replies):
#   8. custom_json op (fee notification)
#   9. c_j_fee ledger - Custom JSON fee
#   10. fee_inc ledger - Fee income recognized
#
# Reply events (keepsats notification - different short_id, linked via replies):
#   11. custom_json op (keepsats balance notification)
#   12. recv_l ledger - Receive Lightning (keepsats balance update)
#
# Reply events (change return - different short_id, linked via replies):
#   13. transfer op (change return to customer)
#   14. cust_h_out ledger - Customer withdrawal of change
# ---------------------------------------------------------------------------

HIVE_TO_KEEPSATS_FLOW = FlowDefinition(
    name="hive_to_keepsats",
    description="Conversion from Hive to Keepsats (sats stored on system)",
    trigger_op_type="transfer",
    stages=[
        # --- Primary stages (same short_id as trigger) ---
        FlowStage(
            name="trigger_transfer",
            event_type="op",
            op_type="transfer",
            group="primary",
        ),
        FlowStage(
            name="customer_hive_in",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_IN,
            group="primary",
        ),
        FlowStage(
            name="hold_keepsats",
            event_type="ledger",
            ledger_type=LedgerType.HOLD_KEEPSATS,
            group="primary",
        ),
        FlowStage(
            name="conv_hive_to_keepsats",
            event_type="ledger",
            ledger_type=LedgerType.CONV_HIVE_TO_KEEPSATS,
            group="primary",
        ),
        FlowStage(
            name="contra_hive_to_keepsats",
            event_type="ledger",
            ledger_type=LedgerType.CONTRA_HIVE_TO_KEEPSATS,
            group="primary",
        ),
        FlowStage(
            name="conv_customer",
            event_type="ledger",
            ledger_type=LedgerType.CONV_CUSTOMER,
            group="primary",
        ),
        FlowStage(
            name="release_keepsats",
            event_type="ledger",
            ledger_type=LedgerType.RELEASE_KEEPSATS,
            group="primary",
        ),
        # --- Fee notification stages (reply group) ---
        FlowStage(
            name="fee_custom_json_op",
            event_type="op",
            op_type="custom_json",
            group="fee_notification",
        ),
        FlowStage(
            name="custom_json_fee",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOM_JSON_FEE,
            group="fee_notification",
        ),
        FlowStage(
            name="fee_income",
            event_type="ledger",
            ledger_type=LedgerType.FEE_INCOME,
            group="fee_notification",
        ),
        # --- Keepsats notification stages (reply group) ---
        FlowStage(
            name="keepsats_notification_op",
            event_type="op",
            op_type="custom_json",
            group="keepsats_notification",
        ),
        FlowStage(
            name="receive_lightning",
            event_type="ledger",
            ledger_type=LedgerType.RECEIVE_LIGHTNING,
            group="keepsats_notification",
        ),
        # --- Change return stages (reply group) ---
        FlowStage(
            name="change_transfer_op",
            event_type="op",
            op_type="transfer",
            group="change_return",
        ),
        FlowStage(
            name="customer_hive_out",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_OUT,
            group="change_return",
        ),
    ],
)


# ---------------------------------------------------------------------------
# Registry of all known flow definitions
# ---------------------------------------------------------------------------

FLOW_DEFINITIONS = {
    "hive_to_keepsats": HIVE_TO_KEEPSATS_FLOW,
}
