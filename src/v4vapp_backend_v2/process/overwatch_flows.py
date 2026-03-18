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
            required=False,  # Notification may be missing if user has no cust_id or conv is zero
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
            required=False,  # Limit order may be missing if exchange conversion fails or is zero
        ),
        FlowStage(
            name="limit_order_create",
            event_type="ledger",
            ledger_type=LedgerType.LIMIT_ORDER_CREATE,
            group="exchange_order",
            required=False,  # Limit order may be missing if exchange conversion fails or is zero
        ),
        # --- Fill order stages ---
        FlowStage(
            name="fill_order_op",
            event_type="op",
            op_type="fill_order",
            group="fill_order",
            required=False,  # Fill order may be missing if limit order is missing or doesn't fill within timeframe
        ),
        FlowStage(
            name="fill_order_net",
            event_type="ledger",
            ledger_type=LedgerType.FILL_ORDER_NET,
            group="fill_order",
            required=False,  # Fill order may be missing if limit order is missing or doesn't fill within timeframe
        ),
    ],
)


# ---------------------------------------------------------------------------
# Keepsats-to-External Lightning flow
# ---------------------------------------------------------------------------
# Flow: User sends a custom_json op instructing the server to pay an external
# Lightning invoice from their keepsats balance.  The system holds the user's
# sats, sends the LND payment, records the withdrawal and any routing fee,
# releases the held sats, and (optionally) sends a notification custom_json.
#
# Primary events (same short_id as trigger):
#   1. custom_json op (trigger — pay-with-keepsats instruction)
#   2. hold_k ledger — Sats held from user into keepsats escrow
#   3. release_k ledger — Held sats released after payment
#
# Payment events (different short_id — payment hash):
#   4. payment op — LND payment sent (SUCCEEDED / FAILED)
#   5. withdraw_l ledger — Debit user, credit External Lightning Payments
#   6. fee_exp ledger — Lightning routing fee
#
# Notification events (reply group — different short_id):
#   7. custom_json op — Notification to customer about completed payment
# ---------------------------------------------------------------------------

KEEPSATS_TO_EXTERNAL_FLOW = FlowDefinition(
    name="keepsats_to_external",
    description="Send Keepsats (sats) to an external Lightning address",
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
            name="hold_keepsats",
            event_type="ledger",
            ledger_type=LedgerType.HOLD_KEEPSATS,
            group="primary",
        ),
        FlowStage(
            name="release_keepsats",
            event_type="ledger",
            ledger_type=LedgerType.RELEASE_KEEPSATS,
            group="primary",
        ),
        # --- Payment stages (different short_id — payment hash) ---
        FlowStage(
            name="payment_op",
            event_type="op",
            op_type="payment",
            group="payment",
        ),
        FlowStage(
            name="withdraw_lightning",
            event_type="ledger",
            ledger_type=LedgerType.WITHDRAW_LIGHTNING,
            group="payment",
        ),
        FlowStage(
            name="fee_expense",
            event_type="ledger",
            ledger_type=LedgerType.FEE_EXPENSE,
            group="payment",
        ),
        # --- Notification stages (reply group) ---
        FlowStage(
            name="notification_custom_json_op",
            event_type="op",
            op_type="custom_json",
            group="notification",
            required=False,  # Notification may be absent if custom_json broadcast fails
        ),
    ],
)


# ---------------------------------------------------------------------------
# Hive-to-Keepsats External Lightning flow
# ---------------------------------------------------------------------------
# Flow: User sends HIVE to the server account with a Lightning invoice in the
# memo.  The system converts the HIVE to keepsats (identical to the normal
# hive_to_keepsats flow) AND immediately pays the external Lightning invoice.
# This is a superset of hive_to_keepsats — it includes all primary/fee/
# notification/change stages plus the external payment stages.
#
# Primary events (same short_id as trigger):
#   1-7. Same as hive_to_keepsats (transfer, cust_h_in, hold_k, h_conv_k,
#        h_contra_k, cust_conv, release_k)
#
# External payment events (different short_id — payment hash):
#   8.  payment op — LND payment sent
#   9.  withdraw_l ledger — Debit user, credit External Lightning Payments
#  10.  fee_exp ledger — Lightning routing fee
#
# Reply events (same as hive_to_keepsats):
#  11-17. Fee notification, keepsats notification, change return
# ---------------------------------------------------------------------------

HIVE_TO_KEEPSATS_EXTERNAL_FLOW = FlowDefinition(
    name="hive_to_keepsats_external",
    description="Hive converted to Keepsats then paid to external Lightning address",
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
        # --- External payment stages (different short_id — payment hash) ---
        FlowStage(
            name="payment_op",
            event_type="op",
            op_type="payment",
            group="payment",
        ),
        FlowStage(
            name="withdraw_lightning",
            event_type="ledger",
            ledger_type=LedgerType.WITHDRAW_LIGHTNING,
            group="payment",
        ),
        FlowStage(
            name="fee_expense",
            event_type="ledger",
            ledger_type=LedgerType.FEE_EXPENSE,
            group="payment",
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
# External-to-Keepsats Lightning flow
# ---------------------------------------------------------------------------
# Flow: An external Lightning invoice is paid to the server and the sats are
# stored in the recipient's keepsats balance.  The system records the deposit,
# creates a keepsats balance notification (custom_json + recv_l) and sends a
# small HIVE notification transfer to the recipient.  For very small amounts
# the HIVE notification is replaced by a custom_json notification, so the
# HIVE transfer stages and the small-notification custom_json are optional.
#
# Primary events (same short_id as trigger — invoice hash):
#   1. invoice op (trigger)
#   2. deposit_l ledger — Deposit Lightning (external sats → system)
#
# Keepsats notification (reply group — different short_id):
#   3. custom_json op — Keepsats balance notification on Hive
#   4. recv_l ledger — RECEIVE_LIGHTNING (sats credited to customer)
#
# HIVE notification (reply group — different short_id, OPTIONAL):
#   5. transfer op — Small HIVE transfer to notify the recipient
#   6. cust_h_out ledger — CUSTOMER_HIVE_OUT
#
# Small-amount notification (reply group — different short_id, OPTIONAL):
#   7. custom_json op — Notification custom_json (replaces HIVE transfer
#      for very small amounts)
# ---------------------------------------------------------------------------

EXTERNAL_TO_KEEPSATS_FLOW = FlowDefinition(
    name="external_to_keepsats",
    description="External Lightning invoice received and stored in keepsats",
    trigger_op_type="invoice",
    stages=[
        # --- Primary stages (same short_id as trigger — invoice hash) ---
        FlowStage(
            name="trigger_invoice",
            event_type="op",
            op_type="invoice",
            group="primary",
        ),
        FlowStage(
            name="deposit_lightning",
            event_type="ledger",
            ledger_type=LedgerType.DEPOSIT_LIGHTNING,
            group="primary",
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
        # --- HIVE notification stages (reply group, optional) ---
        FlowStage(
            name="hive_notification_transfer_op",
            event_type="op",
            op_type="transfer",
            group="hive_notification",
            required=False,
        ),
        FlowStage(
            name="customer_hive_out",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_OUT,
            group="hive_notification",
            required=False,
        ),
        # --- Small-amount notification (optional, replaces HIVE transfer) ---
        FlowStage(
            name="small_notification_custom_json_op",
            event_type="op",
            op_type="custom_json",
            group="small_notification",
            required=False,
        ),
    ],
)


# ---------------------------------------------------------------------------
# Keepsats internal transfer flow
# ---------------------------------------------------------------------------
# Flow: A customer sends a custom_json op instructing the server to transfer
# sats from their keepsats balance to another customer's keepsats balance.
# This is an internal (non-Lightning) transfer between two users on the
# same system.
#
# Primary events (same short_id as trigger):
#   1. custom_json op (trigger — keepsats transfer instruction)
#   2. c_j_trans ledger — CUSTOM_JSON_TRANSFER (debit sender, credit receiver)
#
# Notification events (reply group — different short_id, OPTIONAL):
#   3. custom_json op — Notification to the receiver
# ---------------------------------------------------------------------------

KEEPSATS_INTERNAL_TRANSFER_FLOW = FlowDefinition(
    name="keepsats_internal_transfer",
    description="Internal keepsats transfer between two customers",
    trigger_op_type="custom_json",
    stages=[
        # --- Primary stages ---
        FlowStage(
            name="trigger_custom_json",
            event_type="op",
            op_type="custom_json",
            group="primary",
        ),
        FlowStage(
            name="custom_json_transfer",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOM_JSON_TRANSFER,
            group="primary",
        ),
        # --- Notification stages (reply group, optional) ---
        FlowStage(
            name="notification_custom_json_op",
            event_type="op",
            op_type="custom_json",
            group="notification",
            required=False,
        ),
    ],
)


# ---------------------------------------------------------------------------
# Registry of all known flow definitions
# ---------------------------------------------------------------------------

FLOW_DEFINITIONS = {
    "hive_to_keepsats": HIVE_TO_KEEPSATS_FLOW,
    "hive_to_keepsats_external": HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
    "keepsats_to_hbd": KEEPSATS_TO_HBD_FLOW,
    "keepsats_to_external": KEEPSATS_TO_EXTERNAL_FLOW,
    "external_to_keepsats": EXTERNAL_TO_KEEPSATS_FLOW,
    "keepsats_internal_transfer": KEEPSATS_INTERNAL_TRANSFER_FLOW,
}
