from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Mapping

from pydantic import BaseModel, ConfigDict

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType


class FeeAggregationResult(BaseModel):
    """Result model for fee aggregation pipeline output."""

    cust_id: str
    total_sats: int | Decimal = 0
    total_usd: Decimal = Decimal(0)
    count: int = 0
    hours: int = 0
    breakdown: dict[str, int | Decimal] = {}

    model_config = ConfigDict(extra="allow")


def fee_aggregation_pipeline(
    cust_ids: list[str] | None = None,
    hours: int | None = None,
) -> List[Mapping[str, Any]]:
    """
    Constructs a MongoDB aggregation pipeline to calculate total fee income for each customer, with optional filtering by customer IDs and recent hours.
    The pipeline performs the following steps:
    1. Matches documents with ledger_type "fee_income" and optionally filters by provided customer IDs and timestamp range.
    2. Groups the matched documents by customer ID and credit subcategory, calculating total sats, total USD, and count for each group.
    3. Regroups the results by customer ID, summing totals across subcategories and creating arrays for subsats and subusd.
    4. Projects the final output format, merging the totals and breakdown into a single document.

    Args:
        cust_ids (list[str] | None): Optional list of customer IDs to filter the aggregation. If None, all customers will be included.
        hours (int | None): Optional number of hours to include only recent fees. If None or 0, all fees are included.
    Returns:
        List[Mapping[str, Any]]: The aggregation pipeline as a list of MongoDB aggregation stages.

    """

    match: Dict[str, Any] = {"ledger_type": LedgerType.FEE_INCOME.value}
    if cust_ids:
        match["cust_id"] = {"$in": cust_ids}
    if hours and hours > 0:
        match["timestamp"] = {
            "$gte": {
                "$dateSubtract": {
                    "startDate": "$$NOW",
                    "unit": "hour",
                    "amount": hours,
                }
            }
        }

    return [
        {"$match": match},
        {
            "$group": {
                "_id": {
                    "cust_id": "$cust_id",
                    "credit_sub": "$credit.sub",
                },
                "total_sats": {"$sum": "$credit_conv.sats"},
                "total_usd": {"$sum": "$credit_conv.usd"},
                "count": {"$sum": 1},
            }
        },
        {
            "$group": {
                "_id": "$_id.cust_id",
                "total_sats": {"$sum": "$total_sats"},
                "total_usd": {"$sum": "$total_usd"},
                "count": {"$sum": "$count"},
                "subs": {
                    "$push": {
                        "credit_sub": "$_id.credit_sub",
                        "total_sats": "$total_sats",
                        "total_usd": "$total_usd",
                        "count": "$count",
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "cust_id": "$_id",
                "total_sats": 1,
                "total_usd": 1,
                "count": 1,
                "subs_sats": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$subs",
                            "as": "s",
                            "in": {
                                "k": "$$s.credit_sub",
                                "v": "$$s.total_sats",
                            },
                        }
                    }
                },
                "breakdown": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$subs",
                            "as": "s",
                            "in": {
                                "k": {"$concat": ["$$s.credit_sub", "_sats"]},
                                "v": "$$s.total_sats",
                            },
                        }
                    }
                },
                "subs_usd": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$subs",
                            "as": "s",
                            "in": {
                                "k": {"$concat": ["$$s.credit_sub", "_usd"]},
                                "v": "$$s.total_usd",
                            },
                        }
                    }
                },
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [
                        {
                            "cust_id": "$cust_id",
                            "total_sats": "$total_sats",
                            "total_usd": "$total_usd",
                            "count": "$count",
                            "breakdown": "$breakdown",
                            "hours": hours if hours and hours > 0 else 0,
                        },
                        "$subs_sats",
                        "$subs_usd",
                    ]
                }
            }
        },
        {"$sort": {"total_usd": -1}},
    ]
