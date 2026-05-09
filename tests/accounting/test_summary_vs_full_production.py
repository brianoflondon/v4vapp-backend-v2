"""
Production-only test: verify that `all_account_balances_summary` returns the same
msats/hive/hbd totals as `one_account_balance` for the accounts used by the
dashboard and sanity checks.

Run locally against the production (or staging) database:

    uv run pytest tests/accounting/test_summary_vs_full_production.py -v -s \
        --config-file production.fromhome.config.yaml

This test is automatically SKIPPED in GitHub Actions (GITHUB_ACTIONS env var set)
and when the production config file does not exist on disk.

Accounts checked
----------------
The following accounts are fetched by the dashboard / sanity checks today and
only need final-balance totals (no per-transaction running history):

  VSC Liability / sub=<server_id>     (keepsats_balance with line_items=False)
  VSC Liability / sub="keepsats"      (server_account_balances sanity check)
  External Lightning Payments / sub=<node_name>  (dashboard lnd_info)
  Treasury Lightning / sub=<node_name>           (dashboard lnd_info)
  Customer Deposits Hive / sub=<server_id>       (server_account_hive_balances)
  Traded Deposits Hive / sub=<server_id>         (server_account_hive_balances)
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Skip guard — must be evaluated before any imports that touch the config
# ---------------------------------------------------------------------------

PRODUCTION_CONFIG = "production.fromhome.config.yaml"
PRODUCTION_CONFIG_PATH = Path("config") / PRODUCTION_CONFIG

_in_ci = os.getenv("GITHUB_ACTIONS") == "true"
_config_missing = not PRODUCTION_CONFIG_PATH.exists()

pytestmark = pytest.mark.skipif(
    _in_ci or _config_missing,
    reason=(
        "Production-data test skipped in CI or when production config is absent. "
        f"Looked for: {PRODUCTION_CONFIG_PATH}"
    ),
)

# ---------------------------------------------------------------------------
# Fixtures — set up InternalConfig against production DB
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def module_monkeypatch():
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(autouse=True, scope="module")
async def production_config(module_monkeypatch):
    """Point InternalConfig at the production config file and connect to its DB."""
    from v4vapp_backend_v2.config.setup import InternalConfig
    from v4vapp_backend_v2.database.db_pymongo import DBConn

    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    cfg = InternalConfig(config_filename=PRODUCTION_CONFIG)
    print(f"\nConnected to production config: {cfg.server_id} / node={cfg.node_name}")
    db = DBConn()
    await db.setup_database()
    yield cfg
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Tolerance in msats — summary and full pipelines may differ by a tiny amount
# due to Decimal128 ↔ Decimal conversion in different code paths.
MSATS_TOLERANCE = Decimal("1")
HIVE_TOLERANCE = Decimal("0.001")


def _msats_from_summary(summary, sub: str, account_name: str) -> Decimal:
    """Return the MSATS total for a (name, sub) pair from an AccountBalances summary."""
    match = next(
        (a for a in summary.root if a.name == account_name and a.sub == sub and not a.contra),
        None,
    )
    if match is None:
        return Decimal("0")
    return match.msats


def _hive_from_summary(summary, sub: str, account_name: str) -> Decimal:
    match = next(
        (a for a in summary.root if a.name == account_name and a.sub == sub and not a.contra),
        None,
    )
    return match.hive if match else Decimal("0")


def _hbd_from_summary(summary, sub: str, account_name: str) -> Decimal:
    match = next(
        (a for a in summary.root if a.name == account_name and a.sub == sub and not a.contra),
        None,
    )
    return match.hbd if match else Decimal("0")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_vsc_liability_server_id_msats_match(production_config):
    """
    all_account_balances_summary should return the same msats as one_account_balance
    for VSC Liability / <server_id> — the account used by keepsats_balance(line_items=False).

    Two variants are checked:
    - Raw (no checkpoints): pure pipeline correctness.
    - With fresh checkpoint: validates the production keepsats_balance() code path.
    """
    from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount

    server_id = production_config.server_id
    account = LiabilityAccount(name="VSC Liability", sub=server_id)

    # --- raw comparison (apples-to-apples) ---
    full_raw, summary = await _fetch_both_raw(account, server_id, "VSC Liability")
    diff_raw = abs(full_raw.msats - summary.msats)
    print(
        f"\nVSC Liability ({server_id}) [raw]:"
        f"\n  full.msats    = {full_raw.msats}"
        f"\n  summary.msats = {summary.msats}"
        f"\n  diff          = {diff_raw}"
    )
    assert diff_raw <= MSATS_TOLERANCE, (
        f"raw msats mismatch for VSC Liability/{server_id}: full={full_raw.msats}, summary={summary.msats}"
    )

    # --- checkpoint comparison (matches keepsats_balance() production path) ---
    full_ckpt, summary_ckpt = await _fetch_both_with_checkpoint(
        account, server_id, "VSC Liability"
    )
    diff_ckpt = abs(full_ckpt.msats - summary_ckpt.msats)
    print(
        f"\nVSC Liability ({server_id}) [checkpoint]:"
        f"\n  full.msats    = {full_ckpt.msats}"
        f"\n  summary.msats = {summary_ckpt.msats}"
        f"\n  diff          = {diff_ckpt}"
    )
    assert diff_ckpt <= MSATS_TOLERANCE, (
        f"checkpoint msats mismatch for VSC Liability/{server_id}: full={full_ckpt.msats}, summary={summary_ckpt.msats}"
    )


async def test_vsc_liability_keepsats_msats_match(production_config):
    """
    VSC Liability / keepsats — used by the server_account_balances sanity check.
    Both raw and checkpoint variants are checked.
    """
    from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount

    account = LiabilityAccount(name="VSC Liability", sub="keepsats")

    full_raw, summary = await _fetch_both_raw(account, "keepsats", "VSC Liability")
    diff_raw = abs(full_raw.msats - summary.msats)
    print(
        f"\nVSC Liability (keepsats) [raw]:"
        f"\n  full.msats    = {full_raw.msats}"
        f"\n  summary.msats = {summary.msats}"
        f"\n  diff          = {diff_raw}"
    )
    assert diff_raw <= MSATS_TOLERANCE, (
        f"raw msats mismatch for VSC Liability/keepsats: full={full_raw.msats}, summary={summary.msats}"
    )

    full_ckpt, summary_ckpt = await _fetch_both_with_checkpoint(
        account, "keepsats", "VSC Liability"
    )
    diff_ckpt = abs(full_ckpt.msats - summary_ckpt.msats)
    print(
        f"\nVSC Liability (keepsats) [checkpoint]:"
        f"\n  full.msats    = {full_ckpt.msats}"
        f"\n  summary.msats = {summary_ckpt.msats}"
        f"\n  diff          = {diff_ckpt}"
    )
    assert diff_ckpt <= MSATS_TOLERANCE, (
        f"checkpoint msats mismatch for VSC Liability/keepsats: full={full_ckpt.msats}, summary={summary_ckpt.msats}"
    )


async def test_external_lightning_payments_msats_match(production_config):
    """
    External Lightning Payments — raw-sum comparison.

    The summary pipeline groups entries by (account_type, name, sub, contra, unit).
    For asset accounts like External Lightning Payments, there are both contra=True
    and contra=False entries.  all_account_balances_summary now merges them into a
    single non-contra entry (same as one_account_balance), so summary == full.
    """
    from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount

    node_name = production_config.node_name
    account = AssetAccount(name="External Lightning Payments", sub=node_name)
    full, summary = await _fetch_both_raw(account, node_name, "External Lightning Payments")

    diff = abs(full.msats - summary.msats)
    print(
        f"\nExternal Lightning Payments ({node_name}) [raw]:"
        f"\n  full.msats    = {full.msats}"
        f"\n  summary.msats = {summary.msats}"
        f"\n  diff          = {diff}"
    )
    assert diff <= MSATS_TOLERANCE, (
        f"raw msats mismatch for External Lightning Payments/{node_name}: full={full.msats}, summary={summary.msats}"
    )


async def test_treasury_lightning_msats_match(production_config):
    """
    Treasury Lightning — raw-sum comparison.

    Unlike External Lightning Payments, the all_cust_ids field IS consistently populated
    for Treasury Lightning entries, so the summary pipeline matches the full pipeline.
    """
    from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount

    node_name = production_config.node_name
    account = AssetAccount(name="Treasury Lightning", sub=node_name)
    full, summary = await _fetch_both_raw(account, node_name, "Treasury Lightning")

    diff = abs(full.msats - summary.msats)
    print(
        f"\nTreasury Lightning ({node_name}) [raw]:"
        f"\n  full.msats    = {full.msats}"
        f"\n  summary.msats = {summary.msats}"
        f"\n  diff          = {diff}"
    )
    assert diff <= MSATS_TOLERANCE, (
        f"raw msats mismatch for Treasury Lightning/{node_name}: full={full.msats}, summary={summary.msats}"
    )


async def test_customer_deposits_hive_balances_match(production_config):
    """
    Customer Deposits Hive — used by server_account_hive_balances sanity check.
    Needs .hive and .hbd totals only.  Raw-sum comparison.
    """
    from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount

    server_id = production_config.server_id
    account = AssetAccount(name="Customer Deposits Hive", sub=server_id)
    full, summary = await _fetch_both_raw(account, server_id, "Customer Deposits Hive")

    hive_diff = abs(full.hive - summary.hive)
    hbd_diff = abs(full.hbd - summary.hbd)
    print(
        f"\nCustomer Deposits Hive ({server_id}):"
        f"\n  full.hive={full.hive}, summary.hive={summary.hive}, diff={hive_diff}"
        f"\n  full.hbd={full.hbd},  summary.hbd={summary.hbd},  diff={hbd_diff}"
    )
    assert hive_diff <= HIVE_TOLERANCE, (
        f"HIVE mismatch for Customer Deposits Hive/{server_id}: full={full.hive}, summary={summary.hive}"
    )
    assert hbd_diff <= HIVE_TOLERANCE, (
        f"HBD mismatch for Customer Deposits Hive/{server_id}: full={full.hbd}, summary={summary.hbd}"
    )


async def test_traded_deposits_hive_balances_match(production_config):
    """
    Traded Deposits Hive — used by server_account_hive_balances sanity check.
    Raw-sum comparison.
    """
    from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount

    server_id = production_config.server_id
    account = AssetAccount(name="Traded Deposits Hive", sub=server_id)
    full, summary = await _fetch_both_raw(account, server_id, "Traded Deposits Hive")

    hive_diff = abs(full.hive - summary.hive)
    hbd_diff = abs(full.hbd - summary.hbd)
    print(
        f"\nTraded Deposits Hive ({server_id}):"
        f"\n  full.hive={full.hive}, summary.hive={summary.hive}, diff={hive_diff}"
        f"\n  full.hbd={full.hbd},  summary.hbd={summary.hbd},  diff={hbd_diff}"
    )
    assert hive_diff <= HIVE_TOLERANCE, (
        f"HIVE mismatch for Traded Deposits Hive/{server_id}: full={full.hive}, summary={summary.hive}"
    )
    assert hbd_diff <= HIVE_TOLERANCE, (
        f"HBD mismatch for Traded Deposits Hive/{server_id}: full={full.hbd}, summary={summary.hbd}"
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


async def _fetch_both_raw(account, sub, account_name):
    """
    Compare all_account_balances_summary against one_account_balance(use_checkpoints=False).

    This is an apples-to-apples comparison: both pipelines do a raw full-history sum
    without checkpoints.  Use this to verify pipeline correctness independent of
    whether checkpoint data is fresh.
    """
    import asyncio

    from v4vapp_backend_v2.accounting.account_balances import (
        all_account_balances_summary,
        one_account_balance,
    )

    async with asyncio.TaskGroup() as tg:
        full_task = tg.create_task(
            one_account_balance(account=account, use_cache=False, use_checkpoints=False)
        )
        summary_task = tg.create_task(
            all_account_balances_summary(account_name=account_name, cust_ids={sub})
        )

    full = full_task.result()
    summary_result = summary_task.result()
    summary_match = _extract_summary(summary_result, sub, account_name)
    return full, summary_match


async def _fetch_both_with_checkpoint(account, sub, account_name):
    """
    Compare all_account_balances_summary against one_account_balance(use_checkpoints=True).

    Ensures the checkpoint is fresh first (matching what sanity_checks.py does) then
    compares.  This validates that the summary path agrees with the production
    checkpoint-accelerated path.
    """
    import asyncio

    from v4vapp_backend_v2.accounting.account_balances import (
        all_account_balances_summary,
        one_account_balance,
    )
    from v4vapp_backend_v2.accounting.ledger_checkpoints import (
        PeriodType,
        latest_period_create_checkpoint,
    )

    await latest_period_create_checkpoint(account=account, period_type=PeriodType.DAILY)

    async with asyncio.TaskGroup() as tg:
        full_task = tg.create_task(
            one_account_balance(account=account, use_cache=False, use_checkpoints=True)
        )
        summary_task = tg.create_task(
            all_account_balances_summary(account_name=account_name, cust_ids={sub})
        )

    full = full_task.result()
    summary_result = summary_task.result()
    summary_match = _extract_summary(summary_result, sub, account_name)
    return full, summary_match


def _extract_summary(summary_result, sub, account_name):
    """Extract the non-contra LedgerAccountDetails for (account_name, sub) from an AccountBalances."""
    from v4vapp_backend_v2.accounting.accounting_classes import LedgerAccountDetails
    from v4vapp_backend_v2.accounting.ledger_account_classes import AccountType

    match = next(
        (a for a in summary_result.root if a.sub == sub and not a.contra), None
    )
    if match is None:
        return LedgerAccountDetails(
            name=account_name,
            account_type=AccountType.LIABILITY,
            sub=sub,
        )
    return match
