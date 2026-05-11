"""
Diagnose balance sheet discrepancy.

Usage:
    cd v4vapp-backend-v2
    uv run python scripts/diagnose_balance_sheet.py
"""

import asyncio

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn


async def run() -> None:
    InternalConfig(config_filename="production.fromhome.config.yaml")
    db_conn = DBConn()
    await db_conn.setup_database()

    col = LedgerEntry.collection()

    # ── 1. Balance check ──────────────────────────────────────────────────────
    check_pipeline = [
        {"$match": {"reversed": {"$exists": False}, "conv_signed": {"$exists": True}}},
        {
            "$group": {
                "_id": None,
                "assets_msats": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$debit.account_type", "Asset"]},
                            {"$toDouble": "$conv_signed.debit.msats"},
                            {"$cond": [{"$eq": ["$credit.account_type", "Asset"]}, {"$toDouble": "$conv_signed.credit.msats"}, 0]},
                        ]
                    }
                },
                "liabilities_msats": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$credit.account_type", "Liability"]},
                            {"$toDouble": "$conv_signed.credit.msats"},
                            {"$cond": [{"$eq": ["$debit.account_type", "Liability"]}, {"$toDouble": "$conv_signed.debit.msats"}, 0]},
                        ]
                    }
                },
                "revenue_msats": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$credit.account_type", "Revenue"]},
                            {"$toDouble": "$conv_signed.credit.msats"},
                            {"$cond": [{"$eq": ["$debit.account_type", "Revenue"]}, {"$toDouble": "$conv_signed.debit.msats"}, 0]},
                        ]
                    }
                },
                "expense_msats": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$debit.account_type", "Expense"]},
                            {"$toDouble": "$conv_signed.debit.msats"},
                            {"$cond": [{"$eq": ["$credit.account_type", "Expense"]}, {"$toDouble": "$conv_signed.credit.msats"}, 0]},
                        ]
                    }
                },
                "total_entries": {"$sum": 1},
            }
        },
        {
            "$addFields": {
                "equity_msats": {"$subtract": ["$revenue_msats", "$expense_msats"]},
                "total_msats": {
                    "$subtract": [
                        "$assets_msats",
                        {"$add": ["$liabilities_msats", {"$subtract": ["$revenue_msats", "$expense_msats"]}]},
                    ]
                },
            }
        },
    ]

    print("=" * 70)
    print("1. BALANCE CHECK")
    print("=" * 70)
    cursor = await col.aggregate(check_pipeline)
    async for doc in cursor:
        for k, v in doc.items():
            if k == "_id":
                continue
            sats = v / 1000 if isinstance(v, (int, float)) else v
            print(f"  {k:<25} {v:>22.3f}  ({sats:>16.3f} sats)")

    # ── 2. Notes entries grouped ──────────────────────────────────────────────
    notes_summary = [
        {"$match": {"notes": {"$exists": True, "$ne": ""}}},
        {
            "$group": {
                "_id": {
                    "has_conv_signed": {"$cond": [{"$ifNull": ["$conv_signed", False]}, True, False]},
                    "is_reversed": {"$cond": [{"$ifNull": ["$reversed", False]}, True, False]},
                    "debit_type": "$debit.account_type",
                    "credit_type": "$credit.account_type",
                    "ledger_type": "$ledger_type",
                },
                "count": {"$sum": 1},
                "sum_debit_msats": {"$sum": {"$toDouble": "$conv_signed.debit.msats"}},
                "sum_credit_msats": {"$sum": {"$toDouble": "$conv_signed.credit.msats"}},
            }
        },
        {"$sort": {"_id.ledger_type": 1}},
    ]

    print()
    print("=" * 70)
    print("2. NOTES ENTRIES — grouped")
    print("=" * 70)
    cursor = await col.aggregate(notes_summary)
    async for doc in cursor:
        g = doc["_id"]
        print(
            f"  {g['ledger_type']:<35} DR={g['debit_type']:<12} CR={g['credit_type']:<12}"
            f"  conv={g['has_conv_signed']}  rev={g['is_reversed']}  n={doc['count']}"
            f"  Σdebit={doc['sum_debit_msats']:>14.0f}  Σcredit={doc['sum_credit_msats']:>14.0f}"
        )

    # ── 3. Notes entries per-entry ────────────────────────────────────────────
    print()
    print("=" * 70)
    print("3. NOTES ENTRIES — per entry")
    print("=" * 70)
    cursor = col.find(
        {"notes": {"$exists": True, "$ne": ""}},
        {
            "group_id": 1, "ledger_type": 1, "notes": 1, "reversed": 1,
            "conv_signed.debit.msats": 1, "conv_signed.credit.msats": 1,
            "debit.account_type": 1, "debit.name": 1, "debit.sub": 1,
            "credit.account_type": 1, "credit.name": 1, "credit.sub": 1,
        },
    ).sort("ledger_type", 1)
    async for d in cursor:
        conv = d.get("conv_signed", {})
        dm = conv.get("debit", {}).get("msats")
        cm = conv.get("credit", {}).get("msats")
        dm_f = float(str(dm)) if dm is not None else None
        cm_f = float(str(cm)) if cm is not None else None
        db_acc = f"{d.get('debit',{}).get('account_type')}/{d.get('debit',{}).get('name')}/{d.get('debit',{}).get('sub')}"
        cr_acc = f"{d.get('credit',{}).get('account_type')}/{d.get('credit',{}).get('name')}/{d.get('credit',{}).get('sub')}"
        print(
            f"  {d.get('ledger_type'):<35}  rev={d.get('reversed', False)}"
            f"\n    DR {db_acc:<55} msats={dm_f}"
            f"\n    CR {cr_acc:<55} msats={cm_f}"
            f"\n    notes={d.get('notes')!r}"
        )

    # ── 4. Missing conv_signed ────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("4. ENTRIES MISSING conv_signed (excluded from balance check)")
    print("=" * 70)
    missing_pipeline = [
        {"$match": {"reversed": {"$exists": False}, "conv_signed": {"$exists": False}}},
        {"$group": {"_id": "$ledger_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    cursor = await col.aggregate(missing_pipeline)
    found = False
    async for doc in cursor:
        found = True
        print(f"  {doc['_id']:<40}  n={doc['count']}")
    if not found:
        print("  (none)")

    # ── 5. Net signed sums by ledger_type/account pair ────────────────────────
    print()
    print("=" * 70)
    print("5. LEDGER TYPES WITH NON-ZERO NET (debit_msats + credit_msats)")
    print("   In a balanced system each entry should be self-cancelling (net=0)")
    print("=" * 70)
    net_pipeline = [
        {"$match": {"reversed": {"$exists": False}, "conv_signed": {"$exists": True}}},
        {
            "$group": {
                "_id": {
                    "ledger_type": "$ledger_type",
                    "debit_type": "$debit.account_type",
                    "credit_type": "$credit.account_type",
                },
                "count": {"$sum": 1},
                "sum_debit_msats": {"$sum": {"$toDouble": "$conv_signed.debit.msats"}},
                "sum_credit_msats": {"$sum": {"$toDouble": "$conv_signed.credit.msats"}},
            }
        },
        {
            "$addFields": {
                "net": {"$add": ["$sum_debit_msats", "$sum_credit_msats"]}
            }
        },
        {"$match": {"$expr": {"$gt": [{"$abs": "$net"}, 1000]}}},
        {"$sort": {"net": 1}},
    ]
    cursor = await col.aggregate(net_pipeline)
    async for doc in cursor:
        g = doc["_id"]
        net_sats = doc["net"] / 1000
        print(
            f"  {g['ledger_type']:<40}  DR={g['debit_type']:<12} CR={g['credit_type']:<12}"
            f"  n={doc['count']:>5}  net={doc['net']:>18.0f} msats  ({net_sats:>12.1f} sats)"
        )


if __name__ == "__main__":
    asyncio.run(run())
