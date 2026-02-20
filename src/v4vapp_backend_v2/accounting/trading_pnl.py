from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from v4vapp_backend_v2.accounting.account_balances import list_all_accounts, one_account_balance
from v4vapp_backend_v2.accounting.accounting_classes import AccountBalanceLine
from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType


async def generate_trading_pnl_report(
    as_of_date: Optional[datetime] = None,
    subs: Optional[List[str]] = None,
    age: timedelta = timedelta(days=0),
) -> Dict[str, Any]:
    """Generate trading P&L report for Exchange Holdings, grouped by `sub`.

    Returns a dict with details per-sub and an overall total.
    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)

    # Discover subs if not provided
    if subs is None:
        all_accounts = await list_all_accounts()
        subs = sorted({a.sub for a in all_accounts if a.name == "Exchange Holdings"})

    report: Dict[str, Any] = {"as_of_date": as_of_date.isoformat(), "by_sub": {}, "totals": {}}

    grand = {
        "sells": 0,
        "buys": 0,
        "hive_sold": 0.0,
        "hive_bought": 0.0,
        "sats_received": 0.0,
        "sats_spent": 0.0,
        "net_hive_inventory_change": 0.0,
        "net_sats_cash_generated": 0.0,
        "last_price_used": 0.0,
        "last_usd_per_sat": 0.0,
        "inventory_valuation_sats": 0.0,
        "total_trading_pnl_sats": 0.0,
        "total_trading_pnl_usd": 0.0,
    }

    for sub in subs:
        # Ensure we pass a LedgerAccount (AssetAccount) to one_account_balance —
        # calling code sometimes passes a plain sub string.
        account_obj = AssetAccount(name="Exchange Holdings", sub=sub or "")

        # Fetch account balance details for this sub
        acct = await one_account_balance(account=account_obj, as_of_date=as_of_date, age=age)

        hive_lines: List[AccountBalanceLine] = acct.balances.get("hive", [])
        # Filter exchange conversion trades
        trades = [r for r in hive_lines if r.ledger_type == LedgerType.EXCHANGE_CONVERSION.value]

        # Compute per-sub metrics
        sells = buys = 0
        total_hive_sold = total_hive_bought = 0.0
        total_sats_received = total_sats_spent = 0.0
        last_price = 0.0
        last_usd_per_sat = 0.0

        for t in trades:
            desc = (t.description or "").upper()
            # conv_signed holds signed conversion amounts
            conv = getattr(t, "conv_signed", None)
            hive_amt = (
                float(getattr(conv, "hive", 0)) if conv else float(getattr(t, "amount_signed", 0))
            )
            sats_amt = 0.0
            if conv and getattr(conv, "sats", None) is not None:
                sats_amt = float(conv.sats)
            elif conv and getattr(conv, "msats", None) is not None:
                sats_amt = float(conv.msats) / 1000.0

            # Price (sats per hive) if present
            if conv and getattr(conv, "sats_hive", None) is not None:
                try:
                    last_price = float(conv.sats_hive)
                except Exception:
                    pass
            # track usd-per-sat if available
            if conv and getattr(conv, "usd", None) is not None and sats_amt:
                try:
                    usd_per_sat = float(conv.usd) / float(sats_amt)
                    last_usd_per_sat = usd_per_sat
                except Exception:
                    pass

            if "SELL" in desc:
                sells += 1
                total_hive_sold += hive_amt
                total_sats_received += sats_amt
            elif "BUY" in desc:
                buys += 1
                total_hive_bought += hive_amt
                total_sats_spent += sats_amt
            else:
                # Fallback: infer by sign of hive amount (positive on debit rows in sample data)
                if hive_amt < 0:
                    buys += 1
                    total_hive_bought += abs(hive_amt)
                    total_sats_spent += sats_amt
                else:
                    sells += 1
                    total_hive_sold += abs(hive_amt)
                    total_sats_received += sats_amt

        net_hive_change = total_hive_bought - total_hive_sold
        net_sats_cashflow = total_sats_received - total_sats_spent
        inventory_value_sats = net_hive_change * last_price
        total_pnl_sats = net_sats_cashflow + inventory_value_sats

        sub_report = {
            "summary": {
                "sells": sells,
                "buys": buys,
                "hive_sold": total_hive_sold,
                "hive_bought": total_hive_bought,
                "sats_received": total_sats_received,
                "sats_spent": total_sats_spent,
            },
            "performance": {
                "net_hive_inventory_change": net_hive_change,
                "net_sats_cash_generated": net_sats_cashflow,
                "last_price_used": last_price,
                "inventory_valuation_sats": inventory_value_sats,
                "total_trading_pnl_sats": total_pnl_sats,
            },
            "sub": sub,
        }

        # Aggregate grand totals
        grand["sells"] += sells
        grand["buys"] += buys
        grand["hive_sold"] += total_hive_sold
        grand["hive_bought"] += total_hive_bought
        grand["sats_received"] += total_sats_received
        grand["sats_spent"] += total_sats_spent
        # carry forward last price/Usd if seen
        if last_price:
            grand["last_price_used"] = last_price
        if last_usd_per_sat:
            grand["last_usd_per_sat"] = last_usd_per_sat

        report["by_sub"][sub] = sub_report

    # Compute grand performance
    grand["net_hive_inventory_change"] = grand["hive_bought"] - grand["hive_sold"]
    grand["net_sats_cash_generated"] = grand["sats_received"] - grand["sats_spent"]
    # inventory valuation
    grand["inventory_valuation_sats"] = grand["net_hive_inventory_change"] * grand.get(
        "last_price_used", 0.0
    )
    grand["total_trading_pnl_sats"] = (
        grand["net_sats_cash_generated"] + grand["inventory_valuation_sats"]
    )
    # USD conversion of satoshi pnl if rate available
    if grand.get("last_usd_per_sat"):
        grand["total_trading_pnl_usd"] = (
            grand["total_trading_pnl_sats"] * grand["last_usd_per_sat"]
        )
    else:
        grand["total_trading_pnl_usd"] = 0.0

    report["totals"] = grand
    return report


def trading_pnl_printout(report: Dict[str, Any]) -> str:
    """Return a human-readable text report for trading P&L (sats base)."""
    out = []
    out.append("Trading P&L Report")
    out.append("-" * 80)

    totals = report.get("totals", {})

    # Top aligned table: Counts, Hive (3dp), Sats (integers)
    COL_L = 26
    COL_R = 18
    out.append(f"{'':<{COL_L}}{'SELLS':>{COL_R}}{'BUYS':>{COL_R}}")
    out.append(
        f"{'Total':<{COL_L}}{totals.get('sells', 0):>{COL_R},d}{totals.get('buys', 0):>{COL_R},d}"
    )
    out.append(
        f"{'HIVE (amount)':<{COL_L}}{totals.get('hive_sold', 0):>{COL_R},.3f}{totals.get('hive_bought', 0):>{COL_R},.3f}"
    )
    out.append(
        f"{'SATS (cash)':<{COL_L}}{totals.get('sats_received', 0):>{COL_R},.0f}{totals.get('sats_spent', 0):>{COL_R},.0f}"
    )

    out.append("-" * 80)

    # Lower section — right-aligned numeric values; sats shown as integers, hive 3dp
    out.append(
        f"{'Net Hive Inventory Change:':<40}{totals.get('net_hive_inventory_change', 0):>20,.3f} HIVE"
    )
    out.append(
        f"{'Net Sats Cash Generated:':<40}{totals.get('net_sats_cash_generated', 0):>20,.0f} SATS"
    )
    out.append(f"{'Last Price Used (sats/hive):':<40}{totals.get('last_price_used', 0):>20,.3f}")
    out.append(
        f"{'Inventory Valuation:':<40}{totals.get('inventory_valuation_sats', 0):>20,.0f} SATS"
    )
    out.append(f"{'TOTAL TRADING P&L:':<40}{totals.get('total_trading_pnl_sats', 0):>20,.0f} SATS")
    out.append(
        f"{'TOTAL TRADING P&L (USD):':<40}{totals.get('total_trading_pnl_usd', 0):>20,.2f} USD"
    )
    out.append("\nPer-sub breakdown:\n")

    for sub, sreport in report.get("by_sub", {}).items():
        perf = sreport["performance"]
        out.append(f"Sub: {sub}")
        out.append(
            f"  sells={sreport['summary']['sells']} buys={sreport['summary']['buys']}  hive_sold={sreport['summary']['hive_sold']:.6f} hive_bought={sreport['summary']['hive_bought']:.6f}"
        )
        out.append(
            f"  net_hive_change={perf['net_hive_inventory_change']:.6f}  net_sats_cash={perf['net_sats_cash_generated']:.0f}  pnl_sats={perf['total_trading_pnl_sats']:.3f}"
        )
        out.append("-")

    return "\n".join(out)
