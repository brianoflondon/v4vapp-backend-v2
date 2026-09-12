"""Magi identity rewrite: identity-backfill (PR1) and group_id rename (PR3).

Never calls ``LedgerEntry.save()`` or ``MagiBTCTransferEvent.save()``.
Never writes ``indexer_id``. Default is dry-run.

Usage
-----
    uv run python scripts/rewrite_magi_group_id.py \\
      --config devdocker.config.yaml \\
      --mode dry-run \\
      --report /tmp/magi-group-id-rewrite.json

    uv run python scripts/rewrite_magi_group_id.py \\
      --config devdocker.config.yaml \\
      --mode apply --commit \\
      --report /tmp/magi-group-id-rewrite.json
"""

from __future__ import annotations

import asyncio
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import typer
from bson import ObjectId

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.magi.magi_classes import (
    _DUP_PREFIX,
    _MAGI_LEDGER_TYPES,
    DB_MAGI_BTC_COLLECTION,
    identity_key_from_hash,
    is_old_magi_group_id,
    magi_ledgers_for,
    new_group_id_from_hash,
    select_keeper,
)

app = typer.Typer(add_completion=False)

IDENTITY_INDEX_NAME = "identity_key"
LEDGER_COL = "ledger"
_LEDGER_OLD = re.compile(
    r"^(?P<indexer_id>\d+)_(?P<trx_id>.+)_magi_"
    r"(?P<lt>magi_out|magi_in|fee_inc|magi_chg|funding)$"
)
INCIDENT_TRX = "c5384d3cd49b971967ba5259096fe330c7f42139"


def _iso_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _oid(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return value
    try:
        return ObjectId(str(value))
    except Exception:
        return value


def _ledger_lt(doc: dict) -> str | None:
    lt = doc.get("ledger_type")
    if lt:
        return str(lt)
    gid = str(doc.get("group_id") or "")
    m = _LEDGER_OLD.match(gid)
    if m:
        return m.group("lt")
    if gid.endswith("_magi_out"):
        return "magi_out"
    if gid.endswith("_magi_in"):
        return "magi_in"
    return None


def _quarantine_set(doc: dict, computed_key: str, keeper_id: Any, new_id: str) -> dict[str, Any]:
    stored_gid = str(doc.get("group_id") or "")
    stored_legacy = str(doc.get("legacy_group_id") or stored_gid)
    return {
        "group_id": f"{new_id}{_DUP_PREFIX}{doc.get('_id')}",
        "legacy_group_id": f"{_DUP_PREFIX}{stored_legacy}",
        "identity_key": f"{_DUP_PREFIX}{computed_key}",
        "duplicate_of": keeper_id,
    }


def _write_report(report: dict[str, Any], report_path: str | None) -> None:
    if not report_path:
        return
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str))


def _magi_coll():
    return InternalConfig.db[DB_MAGI_BTC_COLLECTION]


def _ledger_coll():
    return InternalConfig.db[LEDGER_COL]


async def scan_identity_groups(limit: int | None = None) -> dict[str, list[dict]]:
    cursor = _magi_coll().find({})
    groups: dict[str, list[dict]] = defaultdict(list)
    scanned = 0
    async for doc in cursor:
        hash_val = doc.get("indexer_tx_hash") or ""
        if not hash_val:
            continue
        key = identity_key_from_hash(hash_val)
        groups[key].append(doc)
        scanned += 1
        if limit is not None and scanned >= limit:
            break
    return dict(groups)


async def magi_ledger_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for lt in _MAGI_LEDGER_TYPES:
        counts[lt] = await _ledger_coll().count_documents({"ledger_type": lt})
    return counts


async def max_indexer_id() -> int | None:
    doc = await _magi_coll().find_one(filter={}, sort=[("indexer_id", -1)])
    if not doc:
        return None
    return int(doc.get("indexer_id") or 0)


async def _ledgers_for_group(docs: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for d in docs:
        for led in await magi_ledgers_for(d):
            lid = str(led.get("_id"))
            if lid in seen:
                continue
            seen.add(lid)
            out.append(led)
    return out


async def plan_identity_backfill(
    groups: dict[str, list[dict]],
) -> dict[str, Any]:
    """Build a dry-run report: keepers, missing fields, quarantine list."""
    keepers: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    missing_key = 0
    missing_legacy = 0
    already_ok = 0

    for computed_key, docs in groups.items():
        if not docs:
            continue
        if len(docs) == 1:
            keeper = docs[0]
            losers: list[dict] = []
        else:
            keeper = await select_keeper(docs)
            if keeper is None:
                continue
            losers = [d for d in docs if d.get("_id") != keeper.get("_id")]

        needs_key = not keeper.get("identity_key")
        needs_legacy = not keeper.get("legacy_group_id") and bool(keeper.get("group_id"))
        if needs_key:
            missing_key += 1
        if needs_legacy:
            missing_legacy += 1
        if not needs_key and not needs_legacy and not losers:
            already_ok += 1

        keepers.append({
            "_id": str(keeper.get("_id")),
            "identity_key": computed_key,
            "group_id": keeper.get("group_id"),
            "set_identity_key": needs_key,
            "set_legacy_group_id": needs_legacy,
            "legacy_group_id": keeper.get("group_id")
            if needs_legacy
            else keeper.get("legacy_group_id"),
            "indexer_id": keeper.get("indexer_id"),
        })
        for loser in losers:
            quarantine.append({
                "_id": str(loser.get("_id")),
                "keeper_id": str(keeper.get("_id")),
                "identity_key": computed_key,
                "group_id": loser.get("group_id"),
                "indexer_id": loser.get("indexer_id"),
            })

    return {
        "generated_at": _iso_now(),
        "mode": "identity-backfill",
        "groups": len(groups),
        "keepers": keepers,
        "quarantine": quarantine,
        "counts": {
            "groups": len(groups),
            "keepers": len(keepers),
            "quarantine": len(quarantine),
            "missing_identity_key": missing_key,
            "missing_legacy_group_id": missing_legacy,
            "already_ok": already_ok,
        },
    }


async def apply_identity_backfill(plan: dict[str, Any], create_index: bool = True) -> dict[str, Any]:
    """Apply identity_key / legacy_group_id $sets and quarantine losers.

    Does not rename group_id on keepers. Does not write indexer_id.
    """
    coll = _magi_coll()
    applied_keepers = 0
    applied_quarantine = 0

    groups = await scan_identity_groups()
    for computed_key, docs in groups.items():
        if not docs:
            continue
        keeper = docs[0] if len(docs) == 1 else await select_keeper(docs)
        if keeper is None:
            continue
        losers = [d for d in docs if d.get("_id") != keeper.get("_id")]

        sets: dict[str, Any] = {}
        if not keeper.get("identity_key"):
            sets["identity_key"] = computed_key
        if not keeper.get("legacy_group_id") and keeper.get("group_id"):
            sets["legacy_group_id"] = keeper["group_id"]
        if sets:
            await coll.update_one({"_id": keeper["_id"]}, {"$set": sets})
            applied_keepers += 1

        for loser in losers:
            await coll.update_one(
                {"_id": loser["_id"]},
                {
                    "$set": _quarantine_set(
                        loser, computed_key, keeper["_id"], computed_key + "_magi"
                    )
                },
            )
            applied_quarantine += 1

    missing = await coll.count_documents({
        "identity_key": {"$exists": False},
        "indexer_tx_hash": {"$exists": True, "$ne": ""},
    })
    index_created = False
    index_error = None
    if create_index:
        if missing:
            index_error = f"refusing unique identity_key index: {missing} docs still missing key"
        else:
            try:
                await coll.create_index(
                    "identity_key",
                    unique=True,
                    name=IDENTITY_INDEX_NAME,
                )
                index_created = True
            except Exception as e:
                index_error = str(e)

    result = {
        "applied_keepers": applied_keepers,
        "applied_quarantine": applied_quarantine,
        "missing_identity_key_after": missing,
        "index_created": index_created,
        "index_error": index_error,
    }
    plan["apply_result"] = result
    return plan


async def run_identity_backfill(
    *,
    commit: bool,
    limit: int | None,
    report_path: str | None,
    create_index: bool,
) -> dict[str, Any]:
    groups = await scan_identity_groups(limit=limit)
    plan = await plan_identity_backfill(groups)
    plan["dry_run"] = not commit
    if commit:
        plan = await apply_identity_backfill(plan, create_index=create_index)
    _write_report(plan, report_path)
    return plan


async def plan_group_id_rewrite(limit: int | None = None) -> dict[str, Any]:
    """Full rewrite dry-run: identity + magi group_id + MAGI_* ledger ids."""
    groups = await scan_identity_groups(limit=limit)
    snapshot_counts = await magi_ledger_counts()
    snapshot_max = await max_indexer_id()

    list_a: list[dict[str, Any]] = []
    list_b: list[dict[str, Any]] = []
    magi_renames: list[dict[str, Any]] = []
    ledger_renames: list[dict[str, Any]] = []
    skipped_b: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    identity_sets: list[dict[str, Any]] = []
    incident: dict[str, Any] | None = None

    for computed_key, docs in groups.items():
        if not docs:
            continue
        keeper = docs[0] if len(docs) == 1 else await select_keeper(docs)
        if keeper is None:
            continue
        losers = [d for d in docs if d.get("_id") != keeper.get("_id")]
        new_gid = f"{computed_key}_magi"
        hash_val = keeper.get("indexer_tx_hash") or ""
        if hash_val:
            new_gid = new_group_id_from_hash(hash_val)

        if len(docs) > 1:
            list_a.append({
                "identity_key": computed_key,
                "count": len(docs),
                "ids": [str(d.get("_id")) for d in docs],
                "indexer_ids": [d.get("indexer_id") for d in docs],
            })

        all_ledgers = await _ledgers_for_group(docs)
        outs = [l for l in all_ledgers if _ledger_lt(l) == "magi_out"]
        ins = [l for l in all_ledgers if _ledger_lt(l) == "magi_in"]
        is_b = len(outs) > 1 or len(ins) > 1

        planned_ledger: list[dict[str, Any]] = []
        for led in await magi_ledgers_for(keeper):
            old_lgid = str(led.get("group_id") or "")
            m = _LEDGER_OLD.match(old_lgid)
            if not m:
                continue
            new_lgid = f"{new_gid}_{m.group('lt')}"
            existing = await _ledger_coll().find_one({"group_id": new_lgid})
            if existing and existing.get("_id") != led.get("_id"):
                is_b = True
            planned_ledger.append({
                "_id": str(led.get("_id")),
                "old_group_id": old_lgid,
                "new_group_id": new_lgid,
                "ledger_type": _ledger_lt(led),
            })

        if is_b:
            list_b.append({
                "identity_key": computed_key,
                "magi_out": len(outs),
                "magi_in": len(ins),
                "ledger_group_ids": [str(l.get("group_id")) for l in all_ledgers],
            })
            skipped_b.append({
                "identity_key": computed_key,
                "keeper_id": str(keeper.get("_id")),
                "group_id": keeper.get("group_id"),
            })
        else:
            stored_gid = keeper.get("group_id")
            if stored_gid != new_gid and is_old_magi_group_id(stored_gid):
                magi_renames.append({
                    "_id": str(keeper.get("_id")),
                    "identity_key": computed_key,
                    "old_group_id": stored_gid,
                    "new_group_id": new_gid,
                    "legacy_group_id": keeper.get("legacy_group_id") or stored_gid,
                    "indexer_id": keeper.get("indexer_id"),
                    "indexer_tx_hash": hash_val,
                })
            if not keeper.get("identity_key") or (
                is_old_magi_group_id(stored_gid) and not keeper.get("legacy_group_id")
            ):
                identity_sets.append({
                    "_id": str(keeper.get("_id")),
                    "identity_key": computed_key,
                    "legacy_group_id": keeper.get("legacy_group_id") or stored_gid,
                })
            ledger_renames.extend(planned_ledger)
            for loser in losers:
                quarantine.append({
                    "_id": str(loser.get("_id")),
                    "keeper_id": str(keeper.get("_id")),
                    "identity_key": computed_key,
                    "old_group_id": loser.get("group_id"),
                    "old_legacy_group_id": loser.get("legacy_group_id"),
                    "old_identity_key": loser.get("identity_key"),
                    "new_group_id": f"{new_gid}{_DUP_PREFIX}{loser.get('_id')}",
                    "indexer_id": loser.get("indexer_id"),
                })

        if INCIDENT_TRX in computed_key or INCIDENT_TRX in str(hash_val):
            incident = {
                "identity_key": computed_key,
                "keeper_id": str(keeper.get("_id")),
                "group_id": keeper.get("group_id"),
                "new_group_id": new_gid,
                "indexer_id": keeper.get("indexer_id"),
                "skipped_b": is_b,
                "magi_out": len(outs),
            }

    return {
        "generated_at": _iso_now(),
        "mode": "dry-run",
        "snapshot": {
            "magi_ledger_counts": snapshot_counts,
            "max_indexer_id": snapshot_max,
            "magi_btc_docs": sum(len(v) for v in groups.values()),
        },
        "list_a_duplicate_magi": list_a,
        "list_b_duplicate_books": list_b,
        "skipped_b": skipped_b,
        "magi_renames": magi_renames,
        "ledger_renames": ledger_renames,
        "quarantine": quarantine,
        "identity_sets": identity_sets,
        "incident_c5384d3c": incident,
        "counts": {
            "groups": len(groups),
            "list_a": len(list_a),
            "list_b": len(list_b),
            "magi_renames": len(magi_renames),
            "ledger_renames": len(ledger_renames),
            "quarantine": len(quarantine),
            "skipped_b": len(skipped_b),
        },
    }


async def apply_group_id_rewrite(plan: dict[str, Any]) -> dict[str, Any]:
    """Apply magi group_id + MAGI_* ledger renames. Never indexer_id. Never insert ledger."""
    skip_keys = {s["identity_key"] for s in plan.get("skipped_b") or []}
    groups = await scan_identity_groups()
    magi_updated = 0
    ledger_updated = 0
    quarantined = 0
    identity_updated = 0

    for computed_key, docs in groups.items():
        if not docs:
            continue
        keeper = docs[0] if len(docs) == 1 else await select_keeper(docs)
        if keeper is None:
            continue
        losers = [d for d in docs if d.get("_id") != keeper.get("_id")]
        hash_val = keeper.get("indexer_tx_hash") or ""
        new_gid = new_group_id_from_hash(hash_val) if hash_val else f"{computed_key}_magi"

        if computed_key in skip_keys:
            sets: dict[str, Any] = {}
            if not keeper.get("identity_key"):
                sets["identity_key"] = computed_key
            if is_old_magi_group_id(keeper.get("group_id")) and not keeper.get("legacy_group_id"):
                sets["legacy_group_id"] = keeper.get("group_id")
            if sets:
                await _magi_coll().update_one({"_id": keeper["_id"]}, {"$set": sets})
                identity_updated += 1
            continue

        for loser in losers:
            await _magi_coll().update_one(
                {"_id": loser["_id"]},
                {"$set": _quarantine_set(loser, computed_key, keeper["_id"], new_gid)},
            )
            quarantined += 1

        sets = {}
        if not keeper.get("identity_key"):
            sets["identity_key"] = computed_key
        stored_gid = keeper.get("group_id")
        if is_old_magi_group_id(stored_gid):
            if not keeper.get("legacy_group_id"):
                sets["legacy_group_id"] = stored_gid
            if stored_gid != new_gid:
                sets["group_id"] = new_gid
        if sets:
            await _magi_coll().update_one({"_id": keeper["_id"]}, {"$set": sets})
            magi_updated += 1
            if "identity_key" in sets or "legacy_group_id" in sets:
                identity_updated += 1

        for led in await magi_ledgers_for({
            **keeper,
            "group_id": stored_gid,
            "legacy_group_id": keeper.get("legacy_group_id") or stored_gid,
        }):
            old_lgid = str(led.get("group_id") or "")
            m = _LEDGER_OLD.match(old_lgid)
            if not m:
                continue
            new_lgid = f"{new_gid}_{m.group('lt')}"
            if new_lgid == old_lgid:
                continue
            result = await _ledger_coll().update_one(
                {"_id": led["_id"]},
                {"$set": {"group_id": new_lgid}},
            )
            if result.modified_count:
                ledger_updated += 1

    plan["apply_result"] = {
        "magi_updated": magi_updated,
        "ledger_updated": ledger_updated,
        "quarantined": quarantined,
        "identity_updated": identity_updated,
        "skipped_b": len(skip_keys),
    }
    plan["dry_run"] = False
    plan["mode"] = "apply"
    return plan


async def verify_group_id_rewrite(plan: dict[str, Any]) -> dict[str, Any]:
    snapshot = plan.get("snapshot") or {}
    now_counts = await magi_ledger_counts()
    now_max = await max_indexer_id()
    old_counts = snapshot.get("magi_ledger_counts") or {}
    skip_keys = {s["identity_key"] for s in plan.get("skipped_b") or []}

    count_ok = now_counts == old_counts
    max_ok = now_max == snapshot.get("max_indexer_id")

    leftover_old = []
    missing_identity = 0
    cursor = _magi_coll().find({})
    async for doc in cursor:
        gid = str(doc.get("group_id") or "")
        if gid.startswith(_DUP_PREFIX) or _DUP_PREFIX in gid:
            continue
        hash_val = doc.get("indexer_tx_hash") or ""
        key = identity_key_from_hash(hash_val) if hash_val else ""
        if key in skip_keys:
            continue
        if is_old_magi_group_id(gid):
            leftover_old.append({"_id": str(doc.get("_id")), "group_id": gid})
        if not doc.get("identity_key") and hash_val:
            missing_identity += 1

    incident_ok = None
    incident = plan.get("incident_c5384d3c")
    if incident and not incident.get("skipped_b"):
        new_out = f"{INCIDENT_TRX}_1_magi_magi_out"
        n = await _ledger_coll().count_documents({"group_id": new_out})
        incident_ok = n == 1

    result = {
        "counts_unchanged": count_ok,
        "snapshot_counts": old_counts,
        "current_counts": now_counts,
        "max_indexer_id_unchanged": max_ok,
        "snapshot_max_indexer_id": snapshot.get("max_indexer_id"),
        "current_max_indexer_id": now_max,
        "leftover_old_format": leftover_old,
        "missing_identity_key": missing_identity,
        "incident_one_outbound": incident_ok,
        "ok": count_ok and max_ok and not leftover_old and missing_identity == 0,
    }
    plan["verify_result"] = result
    plan["mode"] = "verify"
    return plan


async def rollback_group_id_rewrite(plan: dict[str, Any]) -> dict[str, Any]:
    """Invert report group_id on keepers + ledger. Leave legacy_group_id / identity_key.
    Un-prefix losers from the report.
    """
    magi_rolled = 0
    ledger_rolled = 0
    losers_restored = 0

    for item in plan.get("magi_renames") or []:
        await _magi_coll().update_one(
            {"_id": _oid(item["_id"])},
            {"$set": {"group_id": item["old_group_id"]}},
        )
        magi_rolled += 1

    for item in plan.get("ledger_renames") or []:
        await _ledger_coll().update_one(
            {"_id": _oid(item["_id"])},
            {"$set": {"group_id": item["old_group_id"]}},
        )
        ledger_rolled += 1

    for item in plan.get("quarantine") or []:
        restore: dict[str, Any] = {}
        if item.get("old_group_id"):
            restore["group_id"] = item["old_group_id"]
        if item.get("old_legacy_group_id") is not None:
            restore["legacy_group_id"] = item["old_legacy_group_id"]
        elif item.get("old_group_id"):
            restore["legacy_group_id"] = item["old_group_id"]
        if item.get("old_identity_key") is not None:
            restore["identity_key"] = item["old_identity_key"]
        if restore:
            await _magi_coll().update_one({"_id": _oid(item["_id"])}, {"$set": restore})
            losers_restored += 1

    result = {
        "magi_rolled": magi_rolled,
        "ledger_rolled": ledger_rolled,
        "losers_restored": losers_restored,
    }
    plan["rollback_result"] = result
    plan["mode"] = "rollback"
    return plan


def _print_rewrite_summary(plan: dict[str, Any]) -> None:
    counts = plan.get("counts") or {}
    typer.echo(
        f"mode={plan.get('mode')} dry_run={plan.get('dry_run', True)} "
        f"groups={counts.get('groups')} "
        f"list_a={counts.get('list_a')} list_b={counts.get('list_b')} "
        f"magi_renames={counts.get('magi_renames')} "
        f"ledger_renames={counts.get('ledger_renames')} "
        f"quarantine={counts.get('quarantine')} "
        f"skipped_b={counts.get('skipped_b')}"
    )
    incident = plan.get("incident_c5384d3c")
    if incident:
        typer.echo(f"incident_c5384d3c: {json.dumps(incident, default=str)}")
    if plan.get("apply_result"):
        typer.echo(json.dumps(plan["apply_result"], default=str))
    if plan.get("verify_result"):
        typer.echo(json.dumps(plan["verify_result"], default=str))
    if plan.get("rollback_result"):
        typer.echo(json.dumps(plan["rollback_result"], default=str))


@app.command()
def main(
    config_filename: str = typer.Option(
        ...,
        "--config",
        "-c",
        help="Config yaml in ./config (required). Use the same file the monitors use.",
    ),
    mode: str = typer.Option(
        "dry-run",
        "--mode",
        help="identity-backfill | dry-run | apply | verify | rollback",
    ),
    report: str | None = typer.Option(None, "--report", help="Write JSON report to this path"),
    limit: int | None = typer.Option(None, "--limit", help="Max magi_btc docs to scan"),
    commit: bool = typer.Option(
        False,
        "--commit",
        help="Actually write. Required for apply / identity-backfill writes / rollback.",
    ),
    create_index: bool = typer.Option(
        True,
        "--create-index/--no-create-index",
        help="After identity-backfill commit, create unique identity_key index.",
    ),
) -> None:
    InternalConfig(config_filename=config_filename)

    async def _run() -> None:
        db_conn = DBConn()
        await db_conn.setup_database()
        if mode == "identity-backfill":
            plan = await run_identity_backfill(
                commit=commit,
                limit=limit,
                report_path=report,
                create_index=create_index,
            )
            counts = plan.get("counts", {})
            typer.echo(
                f"identity-backfill dry_run={plan.get('dry_run', True)} "
                f"groups={counts.get('groups')} "
                f"quarantine={counts.get('quarantine')} "
                f"missing_key={counts.get('missing_identity_key')}"
            )
            if plan.get("apply_result"):
                typer.echo(json.dumps(plan["apply_result"], default=str))
            return

        if mode == "dry-run":
            plan = await plan_group_id_rewrite(limit=limit)
            plan["dry_run"] = True
            _write_report(plan, report)
            _print_rewrite_summary(plan)
            return

        if mode == "apply":
            if not commit:
                raise typer.BadParameter("--mode apply requires --commit")
            plan = await plan_group_id_rewrite(limit=limit)
            plan = await apply_group_id_rewrite(plan)
            plan = await verify_group_id_rewrite(plan)
            _write_report(plan, report)
            _print_rewrite_summary(plan)
            return

        if mode == "verify":
            if report:
                plan = json.loads(Path(report).read_text())
            else:
                plan = await plan_group_id_rewrite(limit=limit)
            plan = await verify_group_id_rewrite(plan)
            _write_report(plan, report)
            _print_rewrite_summary(plan)
            return

        if mode == "rollback":
            if not commit:
                raise typer.BadParameter("--mode rollback requires --commit")
            if not report:
                raise typer.BadParameter("--mode rollback requires --report of the apply run")
            plan = json.loads(Path(report).read_text())
            plan = await rollback_group_id_rewrite(plan)
            _write_report(plan, report)
            _print_rewrite_summary(plan)
            return

        raise typer.BadParameter(f"Unknown --mode {mode}")

    asyncio.run(_run())


if __name__ == "__main__":
    app()
