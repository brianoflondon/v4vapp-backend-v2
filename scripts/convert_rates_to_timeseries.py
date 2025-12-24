#!/usr/bin/env python3
"""Utility: convert legacy "rates" collection documents into the
"rates_ts" time-series collection using the default config (devhive.config.yaml).

Usage:
    python scripts/convert_rates_to_timeseries.py [--config CONFIG] [--source SRC] [--target TGT] [--batch-size N] [--dry-run]

Defaults:
    CONFIG   : config/devhive.config.yaml
    SRC      : rates
    TGT      : DB_RATES_COLLECTION from project config (defaults to rates_ts)

The script will:
 - initialize InternalConfig (loads DB/Redis/logging according to the config file)
 - check/create the target collection as a time-series collection if it doesn't exist
 - read documents from the source collection in batches and insert them into the time-series

Note: documents are copied with a new _id (original _id is removed) so repeated runs will re-insert.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from typing import Optional

from bson import json_util
from pymongo import ASCENDING
from pymongo.errors import CollectionInvalid

from v4vapp_backend_v2.config.setup import DB_RATES_COLLECTION, InternalConfig

DEFAULT_CONFIG = "devhive.config.yaml"
DEFAULT_SOURCE = "rates"
DEFAULT_BATCH = 1000


def ensure_timeseries_collection(
    db, name: str, time_field: str = "timestamp", meta_field: Optional[str] = None
):
    """Create a time-series collection if it does not exist.

    If the collection already exists this is a no-op. If it exists but is not
    a time-series collection, we warn and raise.
    """
    if name in db.list_collection_names():
        coll_info = db.command("listCollections", filter={"name": name})
        coll_spec = coll_info.get("cursor", {}).get("firstBatch", [{}])[0]
        options = coll_spec.get("options", {})
        if options.get("timeseries"):
            print(f"Target collection '{name}' already exists as time-series")
            return
        else:
            raise CollectionInvalid(
                f"Collection '{name}' exists and is not a time-series collection"
            )

    ts_opts = {"timeField": time_field}
    if meta_field:
        ts_opts["metaField"] = meta_field
    print(f"Creating time-series collection '{name}' with options: {ts_opts}")
    db.create_collection(name, timeseries=ts_opts)


def _normalize_timestamp(val):
    """Try to normalize a timestamp stored in a document to a tz-aware datetime.

    Supports: datetime, ISO strings, numeric seconds or milliseconds, and
    extended MongoDB JSON formats like {"$date": "..."} or
    {"$date": {"$numberLong": "..."}}.

    Returns None if the value cannot be parsed/normalized.
    """
    # Direct datetime objects
    if isinstance(val, datetime):
        # make tz-aware in UTC if naive
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val

    # Extended JSON: {"$date": ...}
    if isinstance(val, dict) and "$date" in val:
        inner = val["$date"]
        # inner may be ISO string, number (ms), or subdoc {$numberLong: '...'}
        if isinstance(inner, str):
            try:
                txt = inner.replace("Z", "+00:00")
                dt = datetime.fromisoformat(txt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                return None
        if isinstance(inner, (int, float)):
            try:
                v = int(inner)
                # assume milliseconds if very large
                if v > 1_000_000_000_00:
                    return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
                return datetime.fromtimestamp(v, tz=timezone.utc)
            except Exception:
                return None
        if isinstance(inner, dict) and "$numberLong" in inner:
            try:
                v = int(inner["$numberLong"])
                if v > 1_000_000_000_00:
                    return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
                return datetime.fromtimestamp(v, tz=timezone.utc)
            except Exception:
                return None
        return None

    # ISO string
    if isinstance(val, str):
        try:
            # Handle Z
            txt = val.replace("Z", "+00:00")
            dt = datetime.fromisoformat(txt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    # numeric timestamps: seconds or milliseconds
    if isinstance(val, (int, float)):
        try:
            v = int(val)
            # heuristic: > 1e11 => milliseconds
            if v > 1_000_000_000_00:
                return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return None
    return None


def copy_collection_to_timeseries(
    db,
    source: str,
    target: str,
    batch_size: int = 1000,
    dry_run: bool = False,
    allow_epoch: bool = False,
):
    src = db[source]
    tgt = db[target]

    total = src.count_documents({})
    if total == 0:
        print(f"Source collection '{source}' is empty; nothing to do")
        return 0

    print(
        f"Copying {total} documents from '{source}' to time-series '{target}' (batch_size={batch_size})"
    )

    cursor = src.find({}, no_cursor_timeout=True).sort("timestamp", ASCENDING)
    inserted = 0
    batch = []
    skipped_invalid_ts = 0
    try:
        for doc in cursor:
            # Work on a shallow copy to avoid mutating the cursor's document
            new_doc = dict(doc)
            new_doc.pop("_id", None)

            ts_val = new_doc.get("timestamp")
            normalized = _normalize_timestamp(ts_val)
            if normalized is None:
                print("Skipping document with invalid timestamp: %s", new_doc)
                skipped_invalid_ts += 1
                continue

            # By default treat unix epoch as suspicious and skip it unless user allowed it
            epoch_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            if not allow_epoch and normalized == epoch_dt:
                print("Skipping document with epoch timestamp (likely invalid): %s", new_doc)
                skipped_invalid_ts += 1
                continue

            new_doc["timestamp"] = normalized

            batch.append(new_doc)
            if len(batch) >= batch_size:
                if not dry_run:
                    res = tgt.insert_many(batch)
                    print(f"Inserted {len(res.inserted_ids)} docs")
                else:
                    print(f"Dry run - would insert {len(batch)} docs")
                inserted += len(batch)
                batch = []
        if batch:
            if not dry_run:
                res = tgt.insert_many(batch)
                print(f"Inserted {len(res.inserted_ids)} docs")
            else:
                print(f"Dry run - would insert {len(batch)} docs")
            inserted += len(batch)
    finally:
        cursor.close()

    print(
        f"Completed copy: inserted {inserted} documents into '{target}' (skipped invalid timestamps: {skipped_invalid_ts})"
    )
    return inserted


def main(argv=None):
    p = argparse.ArgumentParser(
        description="Convert 'rates' collection into time-series 'rates_ts'"
    )
    p.add_argument(
        "--config", default=DEFAULT_CONFIG, help="config filename (relative to config/)"
    )
    p.add_argument(
        "--source", default=DEFAULT_SOURCE, help="source collection name (default: rates)"
    )
    p.add_argument(
        "--target", default=DB_RATES_COLLECTION, help="target time-series collection name"
    )
    p.add_argument("--batch-size", default=DEFAULT_BATCH, type=int, help="batch size for inserts")
    p.add_argument(
        "--dry-run", action="store_true", help="don't perform writes; show what would be done"
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="perform writes into the target collection (required to actually insert). Without --apply the script only dry-runs",
    )
    p.add_argument(
        "--allow-epoch",
        action="store_true",
        help="allow timestamp values that equal the Unix epoch (1970-01-01T00:00:00Z). By default these are skipped as likely invalid",
    )
    p.add_argument(
        "--backup-file",
        default=None,
        help="optional path to write a JSON-lines backup of the target collection before modifying it",
    )
    args = p.parse_args(argv)

    # Initialize config and database per DBConn.setup_database pattern
    InternalConfig(config_filename=args.config)
    import asyncio

    from v4vapp_backend_v2.database.db_pymongo import DBConn

    db_conn = DBConn()
    # Ensure DB users, collections and timeseries are set up
    asyncio.run(db_conn.setup_database())

    # Use a sync client for bulk copy operations
    db = db_conn.db_sync()

    # Detect metaField if possible: look for any document with 'pair' field
    sample = db[args.source].find_one()
    meta = None
    if sample and "pair" in sample:
        meta = "pair"

    try:
        ensure_timeseries_collection(db, args.target, time_field="timestamp", meta_field=meta)
    except CollectionInvalid as ex:
        print("Target collection exists and is not suitable: %s", ex)
        return 2

    # safety: require --apply to perform writes
    # dry_run is True if user didn't pass --apply OR explicitly passed --dry-run
    if not args.apply:
        print("No --apply flag supplied; running in dry-run mode (no writes will occur)")

    dry_run = (not args.apply) or args.dry_run

    # optional backup of existing target collection before modifications
    if args.backup_file and args.apply:
        print(f"Backing up existing target collection '{args.target}' to {args.backup_file}")
        with open(args.backup_file, "w", encoding="utf-8") as fh:
            for d in db[args.target].find({}):
                fh.write(json_util.dumps(d))
                fh.write("\n")

    inserted = copy_collection_to_timeseries(
        db,
        args.source,
        args.target,
        batch_size=args.batch_size,
        dry_run=dry_run,
        allow_epoch=args.allow_epoch,
    )
    print(f"Inserted {inserted} documents (dry_run={dry_run})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
