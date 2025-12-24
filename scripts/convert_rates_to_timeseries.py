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
import logging
import sys
from typing import Optional

from pymongo import ASCENDING
from pymongo.errors import CollectionInvalid

from v4vapp_backend_v2.config.setup import DB_RATES_COLLECTION, InternalConfig, logger

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
            logger.info(f"Target collection '{name}' already exists as time-series")
            return
        else:
            raise CollectionInvalid(
                f"Collection '{name}' exists and is not a time-series collection"
            )

    ts_opts = {"timeField": time_field}
    if meta_field:
        ts_opts["metaField"] = meta_field
    logger.info(f"Creating time-series collection '{name}' with options: {ts_opts}")
    db.create_collection(name, timeseries=ts_opts)


def copy_collection_to_timeseries(
    db, source: str, target: str, batch_size: int = 1000, dry_run: bool = False
):
    src = db[source]
    tgt = db[target]

    total = src.count_documents({})
    if total == 0:
        logger.info(f"Source collection '{source}' is empty; nothing to do")
        return 0

    logger.info(
        f"Copying {total} documents from '{source}' to time-series '{target}' (batch_size={batch_size})"
    )

    cursor = src.find({}, no_cursor_timeout=True).sort("timestamp", ASCENDING)
    inserted = 0
    batch = []
    try:
        for doc in cursor:
            # Remove _id so Mongo assigns a fresh one in the target collection
            doc.pop("_id", None)
            # Ensure timestamp exists
            if "timestamp" not in doc or doc["timestamp"] is None:
                logger.warning("Skipping document without timestamp: %s", doc)
                continue
            batch.append(doc)
            if len(batch) >= batch_size:
                if not dry_run:
                    res = tgt.insert_many(batch)
                    logger.debug(f"Inserted {len(res.inserted_ids)} docs")
                else:
                    logger.debug(f"Dry run - would insert {len(batch)} docs")
                inserted += len(batch)
                batch = []
        if batch:
            if not dry_run:
                res = tgt.insert_many(batch)
                logger.debug(f"Inserted {len(res.inserted_ids)} docs")
            else:
                logger.debug(f"Dry run - would insert {len(batch)} docs")
            inserted += len(batch)
    finally:
        cursor.close()

    logger.info(f"Completed copy: inserted {inserted} documents into '{target}'")
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
    args = p.parse_args(argv)

    # Initialize config and database per DBConn.setup_database pattern
    InternalConfig(config_filename=args.config)
    from v4vapp_backend_v2.database.db_pymongo import DBConn
    import asyncio

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
        logger.error("Target collection exists and is not suitable: %s", ex)
        return 2

    inserted = copy_collection_to_timeseries(
        db, args.source, args.target, batch_size=args.batch_size, dry_run=args.dry_run
    )
    print(f"Inserted {inserted} documents (dry_run={args.dry_run})")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
