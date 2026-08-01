"""
Voltage node ledger closeout (post Legion cutover).

Read-only by default. Snapshots residual ``sub=voltage`` balances and optionally
posts explicit closeout entries after confirmation.

Why books can lag physical residual
-----------------------------------
``reset_lightning_opening_balance`` only aligns External Lightning to **channel
local** balance. On-chain LND wallet balance was never tracked. As channels close,
sats move on-chain and total node residual can exceed External Lightning / voltage
by that untracked wallet delta. Prefer ``repay-owner --physical-sats R`` so the
gap is recognized, then residual is paid back to Owner Loan.

Strategies (see plans/post-cutover-voltage-accounting.md):

  A freeze              Document only — leave residual as frozen asset (no write).
  B reclass-legion      Move External Lightning residual voltage → legion.
                        Use ONLY if economic sats actually moved into Legion and
                        Legion books do not already include them (else double-count).
  B reclass-exchange    Move residual External Lightning voltage → Exchange Holdings.
  C writeoff            Zero residual External Lightning / voltage vs Owner Loan
                        Payable / voltage (uses book residual only).
  repay-owner           Preferred when returning Voltage residual to owner funding:
                        optional align for untracked on-chain, then full repay vs
                        Owner Loan. Legion untouched.

Usage examples:

  # Snapshot production balances (safe)
  uv run python scripts/script_voltage_node_closeout.py \\
      -c production.fromhome.config.yaml snapshot

  # Prefer: repay owner using measured total residual (channel + on-chain)
  uv run python scripts/script_voltage_node_closeout.py \\
      -c production.fromhome.config.yaml repay-owner --physical-sats 24000000

  # Book-only write-off (ignores untracked on-chain gap)
  uv run python scripts/script_voltage_node_closeout.py \\
      -c production.fromhome.config.yaml writeoff

  # Execute only after confirming physical residual and economic intent
  uv run python scripts/script_voltage_node_closeout.py \\
      -c production.fromhome.config.yaml repay-owner --physical-sats 24000000 \\
      --execute --i-understand

Never re-imports Voltage LND archives into live invoices/payments.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    ExpenseAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.currency_class import Currency

CLOSED_NODE = "voltage"
LIVE_NODE = "legion"
MEMO_TAG = "Voltage node closeout T0"

Strategy = Literal[
    "freeze", "writeoff", "reclass-legion", "reclass-exchange", "repay-owner"
]


@dataclass
class NodeSnapshot:
    external_msats: Decimal
    treasury_msats: Decimal
    owner_loan_msats: Decimal
    fee_exp_msats: Decimal
    routing_income_msats: Decimal
    last_external_ts: datetime | None

    @property
    def external_sats(self) -> Decimal:
        return self.external_msats / Decimal(1000)

    @property
    def owner_loan_sats(self) -> Decimal:
        return self.owner_loan_msats / Decimal(1000)


async def _balance(name: str, sub: str, kind: str = "asset") -> tuple[Decimal, datetime | None]:
    if kind == "asset":
        acc = AssetAccount(name=name, sub=sub)
    elif kind == "liability":
        acc = LiabilityAccount(name=name, sub=sub)
    elif kind == "expense":
        acc = ExpenseAccount(name=name, sub=sub)
    elif kind == "revenue":
        acc = RevenueAccount(name=name, sub=sub)
    else:
        raise ValueError(kind)
    bal = await one_account_balance(acc, use_cache=False, use_checkpoints=True)
    return bal.msats, bal.last_transaction_date


async def snapshot_node(sub: str) -> NodeSnapshot:
    ext, ext_ts = await _balance("External Lightning Payments", sub, "asset")
    tre, _ = await _balance("Treasury Lightning", sub, "asset")
    own, _ = await _balance("Owner Loan Payable", sub, "liability")
    fee, _ = await _balance("Fee Expenses Lightning", sub, "expense")
    try:
        rfi, _ = await _balance("Routing Fee Income", sub, "revenue")
    except Exception:
        rfi = Decimal(0)
    return NodeSnapshot(
        external_msats=ext,
        treasury_msats=tre,
        owner_loan_msats=own,
        fee_exp_msats=fee,
        routing_income_msats=rfi,
        last_external_ts=ext_ts,
    )


def _fmt_sats(msats: Decimal) -> str:
    return f"{(msats / Decimal(1000)):,.3f} sats ({msats:,.0f} msats)"


def print_snapshot(label: str, snap: NodeSnapshot) -> None:
    print(f"\n=== {label} ===")
    print(f"  External Lightning Payments : {_fmt_sats(snap.external_msats)}")
    print(f"  Treasury Lightning          : {_fmt_sats(snap.treasury_msats)}")
    print(f"  Owner Loan Payable          : {_fmt_sats(snap.owner_loan_msats)}")
    print(f"  Fee Expenses Lightning      : {_fmt_sats(snap.fee_exp_msats)}")
    print(f"  Routing Fee Income          : {_fmt_sats(snap.routing_income_msats)}")
    print(f"  Last External Lightning tx  : {snap.last_external_ts}")
    gap = snap.owner_loan_msats - snap.external_msats
    print(f"  Gap OwnerLoan − External    : {_fmt_sats(gap)}")


def compute_close_msats(current_external_msats: Decimal, target_sats: Decimal | None) -> Decimal:
    """
    Amount of External Lightning / voltage to remove from the books.

    target_sats: desired residual economic balance on Voltage after closeout.
      None or 0 → full write-down to zero.
      Positive → leave that many sats on books (partial close / freeze remainder).
    """
    if target_sats is None:
        target_msats = Decimal(0)
    else:
        target_msats = (target_sats * Decimal(1000)).quantize(Decimal("1"))

    if current_external_msats < 0:
        raise SystemExit(
            f"Unexpected negative External Lightning / {CLOSED_NODE}: "
            f"{current_external_msats}. Manual review required."
        )
    if target_msats < 0:
        raise SystemExit("--target-sats cannot be negative")
    if target_msats > current_external_msats:
        raise SystemExit(
            f"--target-sats ({target_sats}) exceeds current External Lightning "
            f"({current_external_msats / 1000} sats). Nothing to close; check numbers."
        )
    close_msats = (current_external_msats - target_msats).quantize(Decimal("1"))
    return close_msats


def _msats_conv(amount_msats: Decimal, quote):
    return CryptoConversion(
        conv_from=Currency.MSATS, value=amount_msats, quote=quote
    ).conversion


async def build_align_onchain_entry(gap_msats: Decimal, quote) -> LedgerEntry:
    """
    Recognize previously untracked on-chain wallet residual as node capital.

    Opening-balance tooling only matched channel local, not wallet balance.
    When channels close, that delta becomes visible on-chain.

    Debit  External Lightning / voltage   (↑ asset to match total residual)
    Credit Owner Loan Payable / voltage   (↑ owner capital still on the node)
    """
    conv = _msats_conv(gap_msats, quote)
    now = datetime.now(tz=timezone.utc)
    short_id = "voltage-align"
    group_id = f"{short_id}-{now.isoformat()}-{LedgerType.FUNDING.value}"
    return LedgerEntry(
        cust_id=CLOSED_NODE,
        short_id=short_id,
        op_type="funding",
        ledger_type=LedgerType.FUNDING,
        group_id=group_id,
        timestamp=now,
        description=(
            f"{MEMO_TAG} — recognize untracked on-chain residual on {CLOSED_NODE} "
            f"(channel-only opening balance never included LND wallet; gap made "
            f"visible as channels closed)"
        ),
        debit=AssetAccount(name="External Lightning Payments", sub=CLOSED_NODE),
        debit_unit=Currency.MSATS,
        debit_amount=gap_msats,
        debit_conv=conv,
        credit=LiabilityAccount(name="Owner Loan Payable", sub=CLOSED_NODE),
        credit_unit=Currency.MSATS,
        credit_amount=gap_msats,
        credit_conv=conv,
        user_memo=MEMO_TAG,
    )


async def build_writeoff_entry(
    close_msats: Decimal,
    quote,
    *,
    description: str | None = None,
    short_id: str = "voltage-close",
) -> LedgerEntry:
    """
    Pay residual External Lightning back to owner funding (reverse FUNDING).

    Debit  Owner Loan Payable / voltage   (↓ liability — capital returned to owner)
    Credit External Lightning / voltage   (↓ asset — node residual leaves books)

    Positive amounts (admin editor rejects amount <= 0).
    """
    conv = _msats_conv(close_msats, quote)
    now = datetime.now(tz=timezone.utc)
    group_id = f"{short_id}-{now.isoformat()}-{LedgerType.FUNDING.value}"
    return LedgerEntry(
        cust_id=CLOSED_NODE,
        short_id=short_id,
        op_type="funding",
        ledger_type=LedgerType.FUNDING,
        group_id=group_id,
        timestamp=now,
        description=description
        or (
            f"{MEMO_TAG} — repay residual External Lightning / {CLOSED_NODE} "
            f"to Owner Loan (owner funding returned; node residual removed)"
        ),
        debit=LiabilityAccount(name="Owner Loan Payable", sub=CLOSED_NODE),
        debit_unit=Currency.MSATS,
        debit_amount=close_msats,
        debit_conv=conv,
        credit=AssetAccount(name="External Lightning Payments", sub=CLOSED_NODE),
        credit_unit=Currency.MSATS,
        credit_amount=close_msats,
        credit_conv=conv,
        user_memo=MEMO_TAG,
    )


async def build_reclass_legion_entry(close_msats: Decimal, quote) -> LedgerEntry:
    """
    Strategy B — reclass External Lightning voltage → legion.

    Debit  External Lightning / legion
    Credit External Lightning / voltage

    WARNING: Do not use if Legion channel/local is already matched by FUNDING
    without this residual — that double-counts LN assets.
    """
    conv = CryptoConversion(
        conv_from=Currency.MSATS, value=close_msats, quote=quote
    ).conversion
    now = datetime.now(tz=timezone.utc)
    short_id = "voltage-reclass"
    group_id = f"{short_id}-legion-{now.isoformat()}-{LedgerType.EXCHANGE_TO_NODE.value}"
    return LedgerEntry(
        cust_id=LIVE_NODE,
        short_id=short_id,
        op_type="funding",
        ledger_type=LedgerType.EXCHANGE_TO_NODE,
        group_id=group_id,
        timestamp=now,
        description=(
            f"{MEMO_TAG} — reclass External Lightning {CLOSED_NODE} → {LIVE_NODE} "
            f"(physical sweep of residual capacity)"
        ),
        debit=AssetAccount(name="External Lightning Payments", sub=LIVE_NODE),
        debit_unit=Currency.MSATS,
        debit_amount=close_msats,
        debit_conv=conv,
        credit=AssetAccount(name="External Lightning Payments", sub=CLOSED_NODE),
        credit_unit=Currency.MSATS,
        credit_amount=close_msats,
        credit_conv=conv,
        user_memo=MEMO_TAG,
    )


async def build_reclass_exchange_entry(
    close_msats: Decimal, quote, exchange_sub: str
) -> LedgerEntry:
    """
    Strategy B variant — residual went to exchange (not Legion channels).

    Debit  Exchange Holdings / <exchange>
    Credit External Lightning / voltage
    """
    conv = CryptoConversion(
        conv_from=Currency.MSATS, value=close_msats, quote=quote
    ).conversion
    now = datetime.now(tz=timezone.utc)
    short_id = "voltage-reclass"
    group_id = f"{short_id}-exch-{now.isoformat()}-{LedgerType.EXCHANGE_TO_NODE.value}"
    return LedgerEntry(
        cust_id=exchange_sub,
        short_id=short_id,
        op_type="funding",
        ledger_type=LedgerType.EXCHANGE_TO_NODE,
        group_id=group_id,
        timestamp=now,
        description=(
            f"{MEMO_TAG} — reclass External Lightning {CLOSED_NODE} → "
            f"Exchange Holdings / {exchange_sub}"
        ),
        debit=AssetAccount(name="Exchange Holdings", sub=exchange_sub),
        debit_unit=Currency.MSATS,
        debit_amount=close_msats,
        debit_conv=conv,
        credit=AssetAccount(name="External Lightning Payments", sub=CLOSED_NODE),
        credit_unit=Currency.MSATS,
        credit_amount=close_msats,
        credit_conv=conv,
        user_memo=MEMO_TAG,
    )


def print_entry(entry: LedgerEntry) -> None:
    print("\n--- Proposed ledger entry ---")
    print(f"  group_id     : {entry.group_id}")
    print(f"  ledger_type  : {entry.ledger_type}")
    print(f"  short_id     : {entry.short_id}")
    print(f"  description  : {entry.description}")
    print(f"  DEBIT        : {entry.debit}  amount={entry.debit_amount} {entry.debit_unit}")
    print(f"  CREDIT       : {entry.credit}  amount={entry.credit_amount} {entry.credit_unit}")


async def cmd_snapshot() -> None:
    voltage = await snapshot_node(CLOSED_NODE)
    legion = await snapshot_node(LIVE_NODE)
    print_snapshot(f"sub={CLOSED_NODE} (disconnected)", voltage)
    print_snapshot(f"sub={LIVE_NODE} (live)", legion)
    total_el = voltage.external_msats + legion.external_msats
    print(f"\nCombined External Lightning: {_fmt_sats(total_el)}")
    print(
        "\nGuidance:\n"
        "  • Opening balance only tracked channel local — on-chain wallet was never\n"
        "    booked. Physical residual (channel + chain) can exceed External Lightning.\n"
        "  • Returning residual to owner: use repay-owner --physical-sats R (align gap,\n"
        "    then Dr Owner Loan / Cr External Lightning for full R). Do NOT use full\n"
        "    Owner Loan balance (~54M) — only residual still on the node.\n"
        "  • Legion ~10M FUNDING is a separate starting loan — leave it alone.\n"
        "  • Leave Treasury / Fee Expenses / Routing Fee Income as historical.\n"
        "  • Customer Keepsats / VSC Liability are not node-scoped — do not touch.\n"
    )


async def cmd_repay_owner(physical_sats: Decimal, execute: bool) -> None:
    """
    Return all measured Voltage residual to owner funding.

    If physical > book External Lightning, first post a FUNDING align for the
    untracked on-chain gap, then repay the full physical amount vs Owner Loan.
    """
    voltage = await snapshot_node(CLOSED_NODE)
    legion = await snapshot_node(LIVE_NODE)
    print_snapshot(f"BEFORE sub={CLOSED_NODE}", voltage)
    print_snapshot(f"BEFORE sub={LIVE_NODE}", legion)

    physical_msats = (physical_sats * Decimal(1000)).quantize(Decimal("1"))
    if physical_msats < 0:
        raise SystemExit("--physical-sats cannot be negative")

    book_msats = voltage.external_msats.quantize(Decimal("1"))
    gap_msats = (physical_msats - book_msats).quantize(Decimal("1"))

    print(f"\nPhysical residual (channel + on-chain): {_fmt_sats(physical_msats)}")
    print(f"Book External Lightning / {CLOSED_NODE}:  {_fmt_sats(book_msats)}")
    if gap_msats > 0:
        print(
            f"Untracked on-chain gap (recognize first): {_fmt_sats(gap_msats)}\n"
            "  (Expected when opening balance only matched channel local.)"
        )
    elif gap_msats < 0:
        print(
            f"\n⚠ Physical residual is BELOW books by {_fmt_sats(-gap_msats)}.\n"
            "  That is not the on-chain-undertracking case. Use writeoff / investigate\n"
            "  before repay-owner, or pass a higher --physical-sats."
        )
        raise SystemExit("Refusing repay-owner when physical < book External Lightning.")
    else:
        print("Physical matches books — single repay entry only.")

    await TrackedBaseModel.update_quote()
    quote = TrackedBaseModel.last_quote

    entries: list[LedgerEntry] = []
    if gap_msats > 0:
        entries.append(await build_align_onchain_entry(gap_msats, quote))
    if physical_msats > 0:
        entries.append(
            await build_writeoff_entry(
                physical_msats,
                quote,
                short_id="voltage-repay",
                description=(
                    f"{MEMO_TAG} — repay full residual on {CLOSED_NODE} "
                    f"({physical_sats:,.0f} sats channel+on-chain) to Owner Loan; "
                    f"Legion starting loan untouched"
                ),
            )
        )
    if not entries:
        print("\nNothing to post (physical residual 0).")
        return

    # Projected: after align EL=physical, OL+=gap; after repay EL=0, OL-=physical
    after_el = Decimal(0)
    after_ol = voltage.owner_loan_msats + gap_msats - physical_msats
    print(
        f"\nAfter repay-owner (projected):\n"
        f"  External Lightning / {CLOSED_NODE} → {_fmt_sats(after_el)}\n"
        f"  Owner Loan Payable / {CLOSED_NODE} → {_fmt_sats(after_ol)}\n"
        f"  (Residual Owner Loan is historical capital that left the node into\n"
        f"   customer Keepsats / ops — not cash still on Voltage.)\n"
        f"  External Lightning / {LIVE_NODE} unchanged → {_fmt_sats(legion.external_msats)}\n"
        f"  Owner Loan / {LIVE_NODE} unchanged → {_fmt_sats(legion.owner_loan_msats)}"
    )

    for entry in entries:
        print_entry(entry)

    if not execute:
        print(
            f"\nDry-run only — {len(entries)} entr{'y' if len(entries) == 1 else 'ies'} "
            "not written. Re-run with --execute --i-understand to post."
        )
        return

    for entry in entries:
        await entry.save()
        logger.info(
            f"Posted {MEMO_TAG} repay-owner group_id={entry.group_id} "
            f"amount_msats={entry.debit_amount}",
            extra={"notification": True},
        )
        print(f"Posted group_id={entry.group_id}")

    after_v = await snapshot_node(CLOSED_NODE)
    after_l = await snapshot_node(LIVE_NODE)
    print_snapshot(f"AFTER sub={CLOSED_NODE}", after_v)
    print_snapshot(f"AFTER sub={LIVE_NODE}", after_l)


async def cmd_closeout(
    strategy: Strategy,
    target_sats: Decimal | None,
    execute: bool,
    exchange_sub: str,
    physical_sats: Decimal | None = None,
) -> None:
    if strategy == "freeze":
        await cmd_snapshot()
        print("Strategy A (freeze): no ledger write. Residual stays under sub=voltage.")
        return

    if strategy == "repay-owner":
        if physical_sats is None:
            raise SystemExit("repay-owner requires --physical-sats (total residual on Voltage).")
        await cmd_repay_owner(physical_sats=physical_sats, execute=execute)
        return

    voltage = await snapshot_node(CLOSED_NODE)
    legion = await snapshot_node(LIVE_NODE)
    print_snapshot(f"BEFORE sub={CLOSED_NODE}", voltage)
    print_snapshot(f"BEFORE sub={LIVE_NODE}", legion)

    close_msats = compute_close_msats(voltage.external_msats, target_sats)
    if close_msats == 0:
        print("\nClose amount is 0 — nothing to post.")
        return

    print(f"\nClose amount: {_fmt_sats(close_msats)}")
    if target_sats is not None:
        print(f"Target residual External Lightning / {CLOSED_NODE}: {target_sats:,.3f} sats")
    else:
        print(f"Target residual External Lightning / {CLOSED_NODE}: 0 sats")

    await TrackedBaseModel.update_quote()
    quote = TrackedBaseModel.last_quote

    if strategy == "writeoff":
        entry = await build_writeoff_entry(close_msats, quote)
        after_el = voltage.external_msats - close_msats
        after_ol = voltage.owner_loan_msats - close_msats
        print(
            f"\nAfter write-off / repay (projected):\n"
            f"  External Lightning / {CLOSED_NODE} → {_fmt_sats(after_el)}\n"
            f"  Owner Loan Payable / {CLOSED_NODE} → {_fmt_sats(after_ol)}\n"
            f"  External Lightning / {LIVE_NODE} unchanged → {_fmt_sats(legion.external_msats)}"
        )
    elif strategy == "reclass-legion":
        print(
            "\n⚠ WARNING: reclass-legion increases External Lightning / legion.\n"
            "  Only use if those sats economically moved into Legion and are NOT\n"
            "  already reflected in Legion's FUNDING / channel match.\n"
            "  Prefer: physical sweep → let reset_lightning_opening_balance fund legion,\n"
            "  then writeoff residual on voltage (strategy C)."
        )
        entry = await build_reclass_legion_entry(close_msats, quote)
        print(
            f"\nAfter reclass (projected):\n"
            f"  External Lightning / {CLOSED_NODE} → {_fmt_sats(voltage.external_msats - close_msats)}\n"
            f"  External Lightning / {LIVE_NODE} → {_fmt_sats(legion.external_msats + close_msats)}"
        )
    elif strategy == "reclass-exchange":
        entry = await build_reclass_exchange_entry(close_msats, quote, exchange_sub)
        print(
            f"\nAfter reclass (projected):\n"
            f"  External Lightning / {CLOSED_NODE} → {_fmt_sats(voltage.external_msats - close_msats)}\n"
            f"  Exchange Holdings / {exchange_sub} increases by {_fmt_sats(close_msats)}"
        )
    else:
        raise SystemExit(f"Unknown strategy: {strategy}")

    print_entry(entry)

    if not execute:
        print("\nDry-run only — no write. Re-run with --execute --i-understand to post.")
        return

    await entry.save()
    logger.info(
        f"Posted {MEMO_TAG} strategy={strategy} amount_msats={close_msats} "
        f"group_id={entry.group_id}",
        extra={"notification": True},
    )
    print(f"\nPosted group_id={entry.group_id}")

    after_v = await snapshot_node(CLOSED_NODE)
    after_l = await snapshot_node(LIVE_NODE)
    print_snapshot(f"AFTER sub={CLOSED_NODE}", after_v)
    print_snapshot(f"AFTER sub={LIVE_NODE}", after_l)


async def async_main(args: argparse.Namespace) -> None:
    InternalConfig(config_filename=args.config)
    await DBConn().setup_database()

    if args.command == "snapshot":
        await cmd_snapshot()
        return

    strategy: Strategy = args.command  # type: ignore[assignment]
    target = (
        Decimal(str(args.target_sats))
        if getattr(args, "target_sats", None) is not None
        else None
    )
    physical = (
        Decimal(str(args.physical_sats))
        if getattr(args, "physical_sats", None) is not None
        else None
    )
    if args.execute and not args.i_understand:
        raise SystemExit("Refusing --execute without --i-understand (safety latch).")
    execute = bool(args.execute and args.i_understand)

    await cmd_closeout(
        strategy=strategy,
        target_sats=target,
        execute=execute,
        exchange_sub=getattr(args, "exchange_sub", "binance_convert"),
        physical_sats=physical,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Voltage node residual accounting closeout (post Legion cutover)."
    )
    p.add_argument(
        "-c",
        "--config",
        default="production.fromhome.config.yaml",
        help="Config under config/ (default: production.fromhome.config.yaml)",
    )
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("snapshot", help="Read-only balance snapshot voltage vs legion")

    def add_exec_flags(sp: argparse.ArgumentParser) -> None:
        sp.add_argument(
            "--dry-run",
            action="store_true",
            help="Print proposed entry only (default when --execute omitted)",
        )
        sp.add_argument(
            "--execute",
            action="store_true",
            help="Actually post the ledger entry (requires --i-understand)",
        )
        sp.add_argument(
            "--i-understand",
            action="store_true",
            help="Safety latch acknowledging economic residual has been verified",
        )

    def add_close_flags(sp: argparse.ArgumentParser) -> None:
        sp.add_argument(
            "--target-sats",
            type=float,
            default=None,
            help=(
                "Desired residual External Lightning / voltage after closeout "
                "(default: 0 = full close of current residual)"
            ),
        )
        add_exec_flags(sp)
        sp.add_argument(
            "--exchange-sub",
            default="binance_convert",
            help="Exchange Holdings sub for reclass-exchange (default: binance_convert)",
        )

    repay = sub.add_parser(
        "repay-owner",
        help=(
            "Preferred: return measured Voltage residual (channel+on-chain) to "
            "Owner Loan; align untracked on-chain gap first if needed"
        ),
    )
    repay.add_argument(
        "--physical-sats",
        type=float,
        required=True,
        help="Total residual still on Voltage (channel local + on-chain wallet), in sats",
    )
    add_exec_flags(repay)

    add_close_flags(
        sub.add_parser(
            "writeoff",
            help="Book-only: zero current External Lightning / voltage vs Owner Loan",
        )
    )
    add_close_flags(
        sub.add_parser(
            "reclass-legion",
            help="Strategy B: reclass EL residual voltage → legion (double-count risk)",
        )
    )
    add_close_flags(
        sub.add_parser(
            "reclass-exchange",
            help="Strategy B: reclass EL residual voltage → Exchange Holdings",
        )
    )
    add_close_flags(
        sub.add_parser(
            "freeze",
            help="Strategy A: snapshot only; leave residual frozen under voltage",
        )
    )
    return p


if __name__ == "__main__":
    parser = build_parser()
    ns = parser.parse_args()
    try:
        asyncio.run(async_main(ns))
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        sys.exit(130)
