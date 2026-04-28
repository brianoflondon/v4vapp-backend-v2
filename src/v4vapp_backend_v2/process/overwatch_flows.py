"""
Overwatch flow definitions.

Each flow definition describes the expected stages (ledger entries and operations)
for a particular transaction type. These are used by FlowInstance to determine
whether a transaction flow completed successfully.
"""

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.process_overwatch import FlowDefinition, FlowEvent, FlowStage


def check_balance_request(event: FlowEvent) -> bool:
    """Return True only when the triggering op is a balance-request transfer."""
    return getattr(event.op, "balance_request", False)


def check_magisats_invoice(event: FlowEvent) -> bool:
    """Return True only for Lightning invoices tagged with #MAGISATS."""
    return bool(getattr(event.op, "is_magisats", False))


def check_not_magisats_invoice(event: FlowEvent) -> bool:
    """Return True for Lightning invoices WITHOUT the #MAGISATS tag (standard flows)."""
    return not bool(getattr(event.op, "is_magisats", False))


def check_vsc_call(event: FlowEvent) -> bool:
    """Return True only for custom_json ops that are VSC calls (cj_id starts with 'vsc.')."""
    cj_id = getattr(event.op, "cj_id", "") or ""
    return cj_id.startswith("vsc.")


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

KEEPSATS_TO_HIVE_FLOW = FlowDefinition(
    name="keepsats_to_hive",
    description="Conversion from Keepsats (sats) to HIVE/HBD via exchange",
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
            required=False,  # Conversion may fail if exchange rate is unavailable or zero
        ),
        FlowStage(
            name="exchange_fees",
            event_type="ledger",
            ledger_type=LedgerType.EXCHANGE_FEES,
            group="primary",
            required=False,  # Zero-fee exchange conversions don't generate this entry
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
            required=False,  # Zero-fee payments don't generate a fee_expense entry
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
            required=False,  # Zero-fee payments don't generate a fee_expense entry
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
            event_filter=check_not_magisats_invoice,
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
# External-to-Hive Lightning flow
# ---------------------------------------------------------------------------
# Flow: An external Lightning invoice is paid to the server, the sats are
# stored in the recipient's keepsats balance, and then immediately converted
# to HIVE and sent back to the customer.  This is a superset of
# external_to_keepsats — it includes all the same stages but the HIVE
# notification transfer and CUSTOMER_HIVE_OUT are required (not optional).
#
# When the invoice arrives, both external_to_keepsats and external_to_hive
# candidates are created.  external_to_keepsats completes at 4/4, and the
# superset grace period keeps external_to_hive alive until the HIVE transfer
# and CUSTOMER_HIVE_OUT arrive to complete it at 6/6.
#
# Primary events (same short_id as trigger — invoice hash):
#   1. invoice op (trigger)
#   2. deposit_l ledger — Deposit Lightning (external sats → system)
#
# Keepsats notification (reply group — different short_id):
#   3. custom_json op — Keepsats balance notification on Hive
#   4. recv_l ledger — RECEIVE_LIGHTNING (sats credited to customer)
#
# HIVE payout (reply group — different short_id, REQUIRED):
#   5. transfer op — HIVE transfer to the customer
#   6. cust_h_out ledger — CUSTOMER_HIVE_OUT
#
# Small-amount notification (reply group — different short_id, OPTIONAL):
#   7. custom_json op — Notification custom_json (replaces HIVE transfer
#      for very small amounts — but in this flow the HIVE transfer IS
#      expected, so this remains optional)
# ---------------------------------------------------------------------------

EXTERNAL_TO_HIVE_FLOW = FlowDefinition(
    name="external_to_hive",
    description="External Lightning invoice received, stored in keepsats, then converted to HIVE",
    trigger_op_type="invoice",
    stages=[
        # --- Primary stages (same short_id as trigger — invoice hash) ---
        FlowStage(
            name="trigger_invoice",
            event_type="op",
            op_type="invoice",
            group="primary",
            event_filter=check_not_magisats_invoice,
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
        # --- HIVE payout stages (reply group, REQUIRED) ---
        FlowStage(
            name="hive_notification_transfer_op",
            event_type="op",
            op_type="transfer",
            group="hive_notification",
        ),
        FlowStage(
            name="customer_hive_out",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_OUT,
            group="hive_notification",
        ),
        # --- Small-amount notification (optional) ---
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
# External-to-Keepsats Lightning flow (loopback / self-payment)
# ---------------------------------------------------------------------------
# Flow: Same as external_to_keepsats but without the Lightning ledger
# entries (deposit_l and recv_l).  In a "loopback" scenario the outbound
# payment from a keepsats-initiated flow lands on the same LND node, so
# process_tracked_event completes without creating Lightning accounting
# ledger entries.  The observable events are just:
#
#   1. invoice op (trigger)
#   2. custom_json op — Keepsats balance notification
#
# Optional HIVE notification / small-notification stages remain the same.
# ---------------------------------------------------------------------------

EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW = FlowDefinition(
    name="external_to_keepsats_loopback",
    description="External Lightning invoice (loopback/self-payment) stored in keepsats",
    trigger_op_type="invoice",
    stages=[
        FlowStage(
            name="trigger_invoice",
            event_type="op",
            op_type="invoice",
            group="primary",
            event_filter=check_not_magisats_invoice,
        ),
        FlowStage(
            name="keepsats_notification_op",
            event_type="op",
            op_type="custom_json",
            group="keepsats_notification",
        ),
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
# External-to-Hive Lightning flow (loopback / self-payment)
# ---------------------------------------------------------------------------
# Flow: Same as external_to_hive but without the Lightning ledger entries
# (deposit_l and recv_l).  This is the loopback/self-payment variant where
# the keepsats deposit is immediately followed by HIVE conversion and
# payout.  The HIVE notification transfer and CUSTOMER_HIVE_OUT are
# required (not optional).
#
#   1. invoice op (trigger)
#   2. custom_json op — Keepsats balance notification
#   3. transfer op — HIVE notification transfer to customer (required)
#   4. cust_h_out ledger — CUSTOMER_HIVE_OUT (required)
# ---------------------------------------------------------------------------

EXTERNAL_TO_HIVE_LOOPBACK_FLOW = FlowDefinition(
    name="external_to_hive_loopback",
    description="External Lightning invoice (loopback/self-payment) converted to HIVE",
    trigger_op_type="invoice",
    stages=[
        FlowStage(
            name="trigger_invoice",
            event_type="op",
            op_type="invoice",
            group="primary",
            event_filter=check_not_magisats_invoice,
        ),
        FlowStage(
            name="keepsats_notification_op",
            event_type="op",
            op_type="custom_json",
            group="keepsats_notification",
        ),
        FlowStage(
            name="hive_notification_transfer_op",
            event_type="op",
            op_type="transfer",
            group="hive_notification",
        ),
        FlowStage(
            name="customer_hive_out",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_OUT,
            group="hive_notification",
        ),
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
# Balance request flow
# ---------------------------------------------------------------------------
# Flow: User sends a small HIVE transfer (e.g. 0.001 HIVE) to the server
# account with memo "balance_request".  The system looks up the user's
# keepsats balance and replies with a HIVE transfer containing the result
# in an encrypted memo JSON (including the balance string and sats amount).
#
# Primary events (same short_id as trigger):
#   1. transfer op (trigger) — the balance-request HIVE transfer
#   2. cust_h_in ledger — Customer deposits HIVE (the marker amount)
#
# Reply events (different short_id — the balance reply transfer):
#   3. transfer op — server reply containing the balance JSON
#   4. cust_h_out ledger — Customer hive out (the reply amount)
# ---------------------------------------------------------------------------

BALANCE_REQUEST_FLOW = FlowDefinition(
    name="balance_request",
    description="Balance request: customer sends HIVE, server replies with balance",
    trigger_op_type="transfer",
    stages=[
        # --- Primary stages (same short_id as trigger) ---
        FlowStage(
            name="trigger_transfer",
            event_type="op",
            op_type="transfer",
            group="primary",
            event_filter=check_balance_request,
        ),
        FlowStage(
            name="customer_hive_in",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_IN,
            group="primary",
        ),
        # --- Balance reply stages (reply group) ---
        FlowStage(
            name="balance_reply_transfer_op",
            event_type="op",
            op_type="transfer",
            group="balance_reply",
        ),
        FlowStage(
            name="customer_hive_out",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_OUT,
            group="balance_reply",
        ),
    ],
)


# ---------------------------------------------------------------------------
# Hive-transfer-triggered keepsats transfer (#paywithsats instruction)
# ---------------------------------------------------------------------------
# Flow: User sends a small HIVE transfer (e.g. 0.001 HIVE) to the server
# account with a memo like "recipient.name #paywithsats:NNNN".  The system
# reads the instruction, broadcasts a KeepsatsTransfer custom_json to move
# NNNN sats from the sender's keepsats balance to the recipient, and
# optionally sends a notification to the recipient.
#
# Primary events (same short_id as trigger):
#   1. transfer op (trigger) — the marker HIVE transfer
#   2. cust_h_in ledger — Customer deposits HIVE (the marker amount)
#
# Reply events (different short_id — the KeepsatsTransfer custom_json):
#   3. custom_json op — the broadcast KeepsatsTransfer
#   4. c_j_trans ledger — keepsats movement recorded
#
# Optional notification (different short_id):
#   5. custom_json op — notification to recipient
# ---------------------------------------------------------------------------

HIVE_TRANSFER_PAYWITHSATS_FLOW = FlowDefinition(
    name="hive_transfer_paywithsats",
    description="Internal keepsats transfer triggered by Hive transfer with #paywithsats memo",
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
        # --- KeepsatsTransfer stages (reply group) ---
        FlowStage(
            name="keepsats_transfer_op",
            event_type="op",
            op_type="custom_json",
            group="keepsats_transfer",
        ),
        FlowStage(
            name="custom_json_transfer",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOM_JSON_TRANSFER,
            group="keepsats_transfer",
        ),
        # --- Notification (optional) ---
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
# Hive transfer failure (refund) flow
# ---------------------------------------------------------------------------
# Flow: User sends HIVE/HBD to the server account, but the system cannot
# process the request (conversion limits exceeded, amount below minimum,
# Lightning decode failure, LND payment failure, etc.).  The system returns
# the full amount to the sender.
#
# This flow has the same stage signatures as balance_request, but without
# the event_filter — it acts as a catch-all for any transfer that ends in
# an immediate refund.  When balance_request completes first (it has an
# event_filter), this candidate is resolved via superset/subset logic.
#
# Primary events (same short_id as trigger):
#   1. transfer op (trigger) — customer sends HIVE to server
#   2. cust_h_in ledger — Customer deposits HIVE
#
# Reply events (different short_id — the refund transfer):
#   3. transfer op — server returns the HIVE
#   4. cust_h_out ledger — Refund recorded
# ---------------------------------------------------------------------------

HIVE_TRANSFER_FAILURE_FLOW = FlowDefinition(
    name="hive_transfer_failure",
    description="Failed Hive transfer: amount returned to sender (conversion limits, decode failure, etc.)",
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
        # --- Refund stages (reply group) ---
        FlowStage(
            name="refund_transfer_op",
            event_type="op",
            op_type="transfer",
            group="refund",
        ),
        FlowStage(
            name="customer_hive_out",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_OUT,
            group="refund",
        ),
    ],
)


# ---------------------------------------------------------------------------
# External Lightning to MagiSats (VSC) forwarding flow
# ---------------------------------------------------------------------------
# Flow: A customer sends an invoice tagged with #MAGISATS.  The server
# receives the Lightning payment, stores the sats temporarily as a VSC
# Liability, then forwards them to the customer's Magi (VSC) wallet via a
# VSC custom_json transfer call.  After a variable delay (typically seconds
# to a few minutes) the Magi indexer confirms the transfer and the server
# records the final accounting entries.
#
# Primary events (same short_id as trigger — invoice hash):
#   1. invoice op (trigger) — #MAGISATS-tagged Lightning invoice
#   2. deposit_l ledger — DEPOSIT_LIGHTNING (sats received into server node)
#
# VSC send events (reply group — different short_id, vsc.call custom_json):
#   3. custom_json op (vsc.call) — VSC transfer call sent to Magi contract
#
# Magi receive events (reply group — different short_id, magi_btc_transfer_event):
#   4. magi_btc_transfer_event op — Magi indexer confirms the transfer
#   5. s_to_exc ledger — SERVER_TO_EXCHANGE (net forwarded amount)
#   6. fee_inc ledger — FEE_INCOME (retained fee)
#
# Magi notification events (reply group — different short_id, OPTIONAL):
#   7. custom_json op — KeepsatsTransfer notification sent back to customer
# ---------------------------------------------------------------------------

EXTERNAL_TO_MAGISATS_FLOW = FlowDefinition(
    name="external_to_magisats",
    description="External Lightning invoice forwarded to MagiSats (VSC) for BTC delivery",
    trigger_op_type="invoice",
    stages=[
        # --- Primary stages (same short_id as trigger — invoice hash) ---
        FlowStage(
            name="trigger_invoice",
            event_type="op",
            op_type="invoice",
            group="primary",
            event_filter=check_magisats_invoice,
        ),
        FlowStage(
            name="deposit_lightning",
            event_type="ledger",
            ledger_type=LedgerType.DEPOSIT_LIGHTNING,
            group="primary",
        ),
        # --- VSC send stage (reply group — vsc.call custom_json from server) ---
        FlowStage(
            name="vsc_magi_send_op",
            event_type="op",
            op_type="custom_json",
            group="vsc_send",
            event_filter=check_vsc_call,
        ),
        # --- Magi receive stages (reply group — magi_btc_transfer_event op) ---
        FlowStage(
            name="magi_btc_transfer_op",
            event_type="op",
            op_type="magi_btc_transfer_event",
            group="magi_receive",
        ),
        FlowStage(
            name="server_to_exchange",
            event_type="ledger",
            ledger_type=LedgerType.SERVER_TO_EXCHANGE,
            group="magi_receive",
        ),
        FlowStage(
            name="fee_income_magi",
            event_type="ledger",
            ledger_type=LedgerType.FEE_INCOME,
            group="magi_receive",
        ),
        # --- Magi notification stage (reply group, OPTIONAL) ---
        FlowStage(
            name="magi_notification_op",
            event_type="op",
            op_type="custom_json",
            group="magi_notification",
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
    "keepsats_to_hive": KEEPSATS_TO_HIVE_FLOW,
    "keepsats_to_external": KEEPSATS_TO_EXTERNAL_FLOW,
    "external_to_keepsats": EXTERNAL_TO_KEEPSATS_FLOW,
    "external_to_hive": EXTERNAL_TO_HIVE_FLOW,
    "external_to_keepsats_loopback": EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW,
    "external_to_hive_loopback": EXTERNAL_TO_HIVE_LOOPBACK_FLOW,
    "external_to_magisats": EXTERNAL_TO_MAGISATS_FLOW,
    "keepsats_internal_transfer": KEEPSATS_INTERNAL_TRANSFER_FLOW,
    "hive_transfer_paywithsats": HIVE_TRANSFER_PAYWITHSATS_FLOW,
    "balance_request": BALANCE_REQUEST_FLOW,
    "hive_transfer_failure": HIVE_TRANSFER_FAILURE_FLOW,
}
