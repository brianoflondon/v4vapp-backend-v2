from typing import Any, Dict, List

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.conversion.exchange_protocol import get_exchange_adapter

# ---------------------------------------------------------------------------
# Quick-action presets (built dynamically from config)
# ---------------------------------------------------------------------------


def _get_exchange_sub() -> str:
    """Resolve the exchange sub-account name from config."""
    try:
        return get_exchange_adapter().exchange_name
    except Exception:
        return "binance_convert"  # safe fallback


def _get_node_name() -> str:
    """Resolve the Lightning node name from config."""
    try:
        return InternalConfig().node_name
    except Exception:
        return "voltage"  # safe fallback


def _build_editor_presets() -> List[Dict[str, Any]]:
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
    ]
