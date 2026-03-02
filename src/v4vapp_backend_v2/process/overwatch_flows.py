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
# Keepsats-to-HBD conversion flow
# ---------------------------------------------------------------------------
# Flow: User sends a custom_json op requesting conversion of keepsats to HBD.
# The system debits the user's keepsats, converts to HBD via exchange rate,
# deducts a fee, sends HBD to the user, and places a rebalancing limit order
# on the Hive internal market (which eventually fills).
#
# Primary events (same short_id as trigger):
#   1. custom_json op (trigger)
#   2. k_contra_h ledger - Contra conversion offset
#   3. c_j_fee_r ledger - Fee refund (fee charged in custom_json, refunded)
#   4. c_j_trans ledger - Custom JSON transfer entry
#   5. fee_inc ledger - Fee income recognized
#   6. cust_conv ledger - Customer conversion (net sats→HBD)
#   7. r_vsc_sats ledger - Reclassify VSC sats
#   8. r_vsc_hive ledger - Reclassify VSC hive
#   9. exc_conv ledger - Exchange conversion
#  10. exc_fee ledger - Exchange fees
#
# Reply events (notification - different short_id, linked via replies):
#  11. custom_json op (notification to customer)
#
# Reply events (HBD transfer - different short_id, linked via replies):
#  12. transfer op (HBD payment to customer)
#  13. cust_h_out ledger - Customer hive out
#
# Exchange rebalance events (different short_id, linked via limit order):
#  14. limit_order_create op (sell HIVE to buy HBD on internal market)
#  15. limit_or ledger - Limit order create entry
#
# Fill order events (virtual op, different short_id):
#  16. fill_order op (order matched on internal market)
#  17. fill_or_n ledger - Fill order net entry
# ---------------------------------------------------------------------------

KEEPSATS_TO_HBD_FLOW = FlowDefinition(
    name="keepsats_to_hbd",
    description="Conversion from Keepsats (sats) to HBD via exchange",
    trigger_op_type="custom_json",
    stages=[
        # --- Primary stages (same short_id as trigger) ---
        FlowStage(
            name="trigger_custom_json",
            event_type="op",
            op_type="custom_json",
            group="primary",
        ),
        FlowStage(
            name="contra_keepsats_to_hive",
            event_type="ledger",
            ledger_type=LedgerType.CONTRA_KEEPSATS_TO_HIVE,
            group="primary",
        ),
        FlowStage(
            name="custom_json_fee_refund",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOM_JSON_FEE_REFUND,
            group="primary",
        ),
        FlowStage(
            name="custom_json_transfer",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOM_JSON_TRANSFER,
            group="primary",
        ),
        FlowStage(
            name="fee_income",
            event_type="ledger",
            ledger_type=LedgerType.FEE_INCOME,
            group="primary",
        ),
        FlowStage(
            name="conv_customer",
            event_type="ledger",
            ledger_type=LedgerType.CONV_CUSTOMER,
            group="primary",
        ),
        FlowStage(
            name="reclassify_vsc_sats",
            event_type="ledger",
            ledger_type=LedgerType.RECLASSIFY_VSC_SATS,
            group="primary",
        ),
        FlowStage(
            name="reclassify_vsc_hive",
            event_type="ledger",
            ledger_type=LedgerType.RECLASSIFY_VSC_HIVE,
            group="primary",
        ),
        FlowStage(
            name="exchange_conversion",
            event_type="ledger",
            ledger_type=LedgerType.EXCHANGE_CONVERSION,
            group="primary",
        ),
        FlowStage(
            name="exchange_fees",
            event_type="ledger",
            ledger_type=LedgerType.EXCHANGE_FEES,
            group="primary",
        ),
        # --- Notification stages (reply group) ---
        FlowStage(
            name="notification_custom_json_op",
            event_type="op",
            op_type="custom_json",
            group="notification",
        ),
        # --- HBD transfer stages (reply group) ---
        FlowStage(
            name="hbd_transfer_op",
            event_type="op",
            op_type="transfer",
            group="hbd_transfer",
        ),
        FlowStage(
            name="customer_hive_out",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_OUT,
            group="hbd_transfer",
        ),
        # --- Exchange order stages ---
        FlowStage(
            name="limit_order_create_op",
            event_type="op",
            op_type="limit_order_create",
            group="exchange_order",
        ),
        FlowStage(
            name="limit_order_create",
            event_type="ledger",
            ledger_type=LedgerType.LIMIT_ORDER_CREATE,
            group="exchange_order",
        ),
        # --- Fill order stages ---
        FlowStage(
            name="fill_order_op",
            event_type="op",
            op_type="fill_order",
            group="fill_order",
        ),
        FlowStage(
            name="fill_order_net",
            event_type="ledger",
            ledger_type=LedgerType.FILL_ORDER_NET,
            group="fill_order",
        ),
    ],
)


# ---------------------------------------------------------------------------
# Registry of all known flow definitions
# ---------------------------------------------------------------------------

FLOW_DEFINITIONS = {
    "hive_to_keepsats": HIVE_TO_KEEPSATS_FLOW,
    "keepsats_to_hbd": KEEPSATS_TO_HBD_FLOW,
}
