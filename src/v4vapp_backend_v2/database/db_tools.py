from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Mapping, Optional

from bson import Decimal128, ObjectId
from pymongo.asynchronous.collection import AsyncCollection

from v4vapp_backend_v2.config.decorators import async_time_decorator


def convert_decimal128_to_decimal(value: Any) -> Any:
    """
    This function handles the conversion of Decimal128 values (commonly used in MongoDB)
    to Python Decimal objects, ensuring compatibility with Pydantic models. It processes
    nested structures like dictionaries and lists by applying the conversion recursively.

    Args:
        value (Any): The input value to convert. Can be a Decimal128, dict, list, or any other type.

    Returns:
        Any: The converted value with Decimal128 instances replaced by Decimal objects.
             Dictionaries and lists are processed recursively; other types are returned unchanged.
    """
    if isinstance(value, Decimal128):
        return Decimal(str(value))
    elif isinstance(value, dict):
        return {k: convert_decimal128_to_decimal(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [convert_decimal128_to_decimal(item) for item in value]
    else:
        return value


def convert_object_ids(o: Any) -> None:
    """
    Recursively convert any ObjectId instances found in a nested dict/list structure to their string representation.
    This function mutates the input structure in-place. It walks dictionaries and lists recursively and replaces
    values that are instances of the global ObjectId with str(value). Non-dict/list values are left unchanged.
    If the global ObjectId is None (not available), no conversion is performed.
    Parameters
    ----------
    o : Any
        The object to process. Typically a dict or list (possibly nested) containing ObjectId instances.
    Returns
    -------
    None
        The function returns None and modifies the input object in-place.
    Notes
    -----
    - Only dict and list containers are traversed; other container types are not handled.
    - The function performs a naive recursion and does not protect against cyclic references.
    - Intended for preparing Mongo-style documents for JSON serialization by ensuring ObjectId values become strings.
    """

    if isinstance(o, dict):
        for k, v in list(o.items()):
            if ObjectId is not None and isinstance(v, ObjectId):
                o[k] = str(v)
            else:
                convert_object_ids(v)
    elif isinstance(o, list):
        for i in range(len(o)):
            v = o[i]
            if ObjectId is not None and isinstance(v, ObjectId):
                o[i] = str(v)
            else:
                convert_object_ids(v)


@async_time_decorator
async def find_nearest_by_timestamp(
    collection: AsyncCollection,
    target: datetime,
    ts_field: str = "timestamp",
    max_window: Optional[timedelta] = None,
    filter_extra: Optional[Mapping[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Find the document in a time-series collection whose timestamp field is nearest to
    `target`.

    This function uses two index-backed queries (one before-or-equal, one after-or-equal)
    and compares their distances to the target. It is efficient for large collections so
    long as there is an index on `ts_field` (e.g. `db.rates.create_index([("timestamp", 1)])`).

    Args:
        collection (AsyncCollection): The pymongo async collection to query.
        target (datetime): The datetime to compare against.
        ts_field (str): The name of the timestamp field in documents (default: "timestamp").
        max_window (Optional[timedelta]): If provided, only documents within
            `target +/- max_window` are considered. If no documents are found within this
            window, `None` is returned.

    Returns:
        Optional[Dict[str, Any]]: The nearest document as a Python dict (with any
        Decimal128 values converted to Decimal), or `None` if no document is found.

    Raises:
        ValueError: if `target` is not a datetime instance.
    """
    if not isinstance(target, datetime):
        raise ValueError("target must be a datetime instance")

    # Build bounded queries if a max_window was supplied
    if max_window is not None:
        start = target - max_window
        end = target + max_window
        before_filter = {ts_field: {"$lte": target, "$gte": start}}
        after_filter = {ts_field: {"$gte": target, "$lte": end}}
    else:
        before_filter = {ts_field: {"$lte": target}}
        after_filter = {ts_field: {"$gte": target}}

    if filter_extra:
        before_filter.update(filter_extra)
        after_filter.update(filter_extra)

    # Use index-backed queries: latest <= target and earliest >= target
    before_docs = (
        await collection.find(before_filter).sort(ts_field, -1).limit(1).to_list(length=1)
    )
    after_docs = await collection.find(after_filter).sort(ts_field, 1).limit(1).to_list(length=1)

    candidate = None
    if before_docs and after_docs:
        b = before_docs[0]
        a = after_docs[0]
        b_dt = b.get(ts_field)
        a_dt = a.get(ts_field)
        # If timestamps missing, prefer the one that exists
        if b_dt is None and a_dt is None:
            return None
        if b_dt is None:
            candidate = a
        elif a_dt is None:
            candidate = b
        else:
            # Choose the closer one (prefer before on exact ties)
            if (target - b_dt) <= (a_dt - target):
                candidate = b
            else:
                candidate = a
    elif before_docs:
        candidate = before_docs[0]
    elif after_docs:
        candidate = after_docs[0]
    else:
        return None

    return convert_decimal128_to_decimal(candidate)


@async_time_decorator
async def find_nearest_by_timestamp_server_side(
    collection: AsyncCollection,
    target: datetime,
    ts_field: str = "timestamp",
    max_window: Optional[timedelta] = None,
    filter_extra: Optional[Mapping[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Server-side aggregation version of `find_nearest_by_timestamp` that computes the
    nearest document to `target` inside the database using an aggregation pipeline.

    This builds an aggregation pipeline that optionally matches a bounded window and
    additional filters, computes an absolute difference (delta) between the document
    timestamp and `target`, sorts by delta (and by `ts_field` ascending to prefer
    earlier timestamps on exact ties), and returns the single nearest document.

    The returned document is passed through `convert_decimal128_to_decimal` to
    convert BSON Decimal128 values to Python Decimal objects.
    """
    if not isinstance(target, datetime):
        raise ValueError("target must be a datetime instance")

    # If the collection does not support aggregation (e.g., test fakes),
    # fall back to the index-backed two-query implementation
    if not hasattr(collection, "aggregate"):
        return await find_nearest_by_timestamp(
            collection, target, ts_field=ts_field, max_window=max_window, filter_extra=filter_extra
        )

    pipeline = []

    # Build match stage if required (window or extra filters). Include a check to
    # exclude documents with a null timestamp to avoid subtraction errors.
    match: Dict[str, Any] = {}
    if filter_extra:
        match.update(filter_extra)

    if max_window is not None:
        start = target - max_window
        end = target + max_window
        # make sure timestamps are inside the window
        match.update({ts_field: {"$gte": start, "$lte": end, "$ne": None}})
    else:
        # Ensure timestamp exists and is not null
        match.setdefault(ts_field, {})
        match[ts_field].update({"$ne": None})

    if match:
        pipeline.append({"$match": match})

    # Compute absolute difference (MongoDB subtract on dates yields milliseconds)
    pipeline.append({"$addFields": {"delta": {"$abs": {"$subtract": [f"${ts_field}", target]}}}})

    # Sort by delta ascending; on ties prefer earlier timestamps (ts_field ascending)
    pipeline.append({"$sort": {"delta": 1, ts_field: 1}})

    pipeline.append({"$limit": 1})

    import inspect

    # collection.aggregate may be sync (test fakes) or async (real AsyncCollection).
    agg = collection.aggregate(pipeline)
    # If aggregate returned an awaitable (coroutine), await it to get the cursor
    cursor = await agg if inspect.isawaitable(agg) else agg
    # cursor is expected to support to_list(length=...)
    docs = await cursor.to_list(length=1)  # type: ignore[attr-defined]
    if not docs:
        return None

    candidate = docs[0]
    return convert_decimal128_to_decimal(candidate)
