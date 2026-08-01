from typing import Any

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.config.setup import InternalConfig, StartupFailure
from v4vapp_backend_v2.conversion.exchange_protocol import get_exchange_adapter

# ---------------------------------------------------------------------------
# Quick-action presets (built dynamically from config)
# ---------------------------------------------------------------------------

# Broad catch so preset UI never crashes admin; includes StartupFailure from InternalConfig.
_PRESET_CONFIG_ERRORS = (
    AttributeError,
    TypeError,
    ValueError,
    KeyError,
    StartupFailure,
    RuntimeError,
)


def _get_exchange_sub() -> str:
    """Resolve the exchange sub-account name from config (safe for admin UI)."""
    try:
        return get_exchange_adapter().exchange_name
    except _PRESET_CONFIG_ERRORS:
        return "binance_convert"


def _get_node_name() -> str:
    """
    Resolve the Lightning node / ledger sub name from config (safe for admin UI).

    Prefer ``lnd_config.default`` over a hard-coded historical node name so
    cutovers (e.g. voltage → umbrel) do not invent the wrong sub-account.
    """
    try:
        ic = InternalConfig()
        name = ic.node_name
        if name:
            return name
        default = getattr(ic.config.lnd_config, "default", None)
        if default:
            return str(default)
    except _PRESET_CONFIG_ERRORS:
        pass
    return "unknown"


def _build_editor_presets() -> list[dict[str, Any]]:
    """Build presets using the config-driven exchange name."""
    exchange_sub = _get_exchange_sub()
    node_name = _get_node_name()
    return [
        {
            "id": "exchange_to_lightning",
            "label": f"Exchange → Lightning ({exchange_sub}→{node_name})",
            "icon": "⚡",
            "description": (
                f"Move sats from Exchange Holdings ({exchange_sub}) "
                f"to External Lightning Payments ({node_name}). "
                f"Records withdrawal fee paid to {exchange_sub}."
            ),
            "entries": [
                {
                    "ledger_type": LedgerType.EXCHANGE_TO_NODE.value,
                    "description": f"Transfer sats from {exchange_sub} to {node_name} node",
                    "debit_account_type": "Asset",
                    "debit_name": "External Lightning Payments",
                    "debit_sub": node_name,
                    "credit_account_type": "Asset",
                    "credit_name": "Exchange Holdings",
                    "credit_sub": exchange_sub,
                    "currency": "sats",
                    "cust_id": node_name,
                },
                {
                    "ledger_type": LedgerType.EXCHANGE_FEES.value,
                    "description": "Exchange Withdrawal fee paid",
                    "debit_account_type": "Expense",
                    "debit_name": "Withdrawal Fees Paid",
                    "debit_sub": exchange_sub,
                    "credit_account_type": "Asset",
                    "credit_name": "Exchange Holdings",
                    "credit_sub": exchange_sub,
                    "currency": "sats",
                    "cust_id": exchange_sub,
                },
            ],
        },
        {
            "id": "exchange_fee",
            "label": "Exchange Withdrawal Fee",
            "icon": "💸",
            "description": (f"Record a fee charged by {exchange_sub} for a withdrawal."),
            "entries": [
                {
                    "ledger_type": LedgerType.EXCHANGE_FEES.value,
                    "description": "Exchange Withdrawal fee paid",
                    "debit_account_type": "Expense",
                    "debit_name": "Withdrawal Fees Paid",
                    "debit_sub": exchange_sub,
                    "credit_account_type": "Asset",
                    "credit_name": "Exchange Holdings",
                    "credit_sub": exchange_sub,
                    "currency": "sats",
                    "cust_id": exchange_sub,
                },
            ],
        },
        {
            "id": "voltage_closeout_writeoff",
            "label": "Voltage closeout — write-off residual (vs Owner Loan)",
            "icon": "🧊",
            "description": (
                "Post-cutover Strategy C: zero residual External Lightning Payments / voltage "
                "against Owner Loan Payable / voltage when the Voltage node is empty and "
                "Legion already has its own FUNDING. Amount = current External Lightning / "
                "voltage (sats). Prefer scripts/script_voltage_node_closeout.py for dry-run."
            ),
            "entries": [
                {
                    "ledger_type": LedgerType.FUNDING.value,
                    "description": (
                        "Voltage node closeout T0 — write-off residual External Lightning / "
                        "voltage vs Owner Loan"
                    ),
                    "debit_account_type": "Liability",
                    "debit_name": "Owner Loan Payable",
                    "debit_sub": "voltage",
                    "credit_account_type": "Asset",
                    "credit_name": "External Lightning Payments",
                    "credit_sub": "voltage",
                    "currency": "sats",
                    "cust_id": "voltage",
                },
            ],
        },
        {
            "id": "voltage_reclass_legion",
            "label": "Voltage closeout — reclass residual → Legion",
            "icon": "🔀",
            "description": (
                "Post-cutover Strategy B: reclass External Lightning / voltage into "
                f"External Lightning / {node_name}. ONLY if residual sats economically "
                "moved into Legion and are not already in Legion FUNDING (else double-count)."
            ),
            "entries": [
                {
                    "ledger_type": LedgerType.EXCHANGE_TO_NODE.value,
                    "description": (
                        f"Voltage node closeout T0 — reclass External Lightning voltage → {node_name}"
                    ),
                    "debit_account_type": "Asset",
                    "debit_name": "External Lightning Payments",
                    "debit_sub": node_name,
                    "credit_account_type": "Asset",
                    "credit_name": "External Lightning Payments",
                    "credit_sub": "voltage",
                    "currency": "sats",
                    "cust_id": node_name,
                },
            ],
        },
    ]
