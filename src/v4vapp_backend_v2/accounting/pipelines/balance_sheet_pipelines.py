from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence


def balance_sheet_check_pipeline(
    as_of_date: datetime = datetime.now(tz=timezone.utc), age: timedelta | None = None
) -> Sequence[Mapping[str, Any]]:
    """
    Check if the balance sheet is balanced.

    Args:
        balance_sheet (Dict[str, Dict[str, Dict[str, float]]]): The balance sheet to check.

    Returns:
        bool: True if the balance sheet is balanced, False otherwise.
    """
    if age:
        start_date = as_of_date - age
        date_range_query = {"$gte": start_date, "$lte": as_of_date}
    else:
        date_range_query = {"$lte": as_of_date}

    check_balance_pipeline: Sequence[Mapping[str, Any]] = [
        {"$match": {"conv_signed": {"$exists": True}}},
        {"$match": {"reversed": {"$exists": False}}},
        {
            "$match": {
                "timestamp": date_range_query,
            }
        },
        {
            "$group": {
                "_id": None,
                "assets_msats": {
                    "$sum": {
                        "$add": [
                            {
                                "$cond": [
                                    {"$eq": ["$debit.account_type", "Asset"]},
                                    "$conv_signed.debit.msats",
                                    0,
                                ]
                            },
                            {
                                "$cond": [
                                    {"$eq": ["$credit.account_type", "Asset"]},
                                    "$conv_signed.credit.msats",
                                    0,
                                ]
                            },
                        ]
                    }
                },
                "liabilities_msats": {
                    "$sum": {
                        "$add": [
                            {
                                "$cond": [
                                    {"$eq": ["$debit.account_type", "Liability"]},
                                    "$conv_signed.debit.msats",
                                    0,
                                ]
                            },
                            {
                                "$cond": [
                                    {"$eq": ["$credit.account_type", "Liability"]},
                                    "$conv_signed.credit.msats",
                                    0,
                                ]
                            },
                        ]
                    }
                },
                "revenue_msats": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$credit.account_type", "Revenue"]},
                            "$conv_signed.credit.msats",
                            0,
                        ]
                    }
                },
                "expense_msats": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$debit.account_type", "Expense"]},
                            "$conv_signed.debit.msats",
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "assets_msats": 1,
                "liabilities_msats": 1,
                "equity_msats": {"$subtract": ["$revenue_msats", "$expense_msats"]},
                "total_msats": {
                    "$subtract": [
                        "$assets_msats",
                        {
                            "$add": [
                                "$liabilities_msats",
                                {"$subtract": ["$revenue_msats", "$expense_msats"]},
                            ]
                        },
                    ]
                },
            }
        },
    ]

    return check_balance_pipeline


def balance_sheet_pipeline(
    as_of_date: datetime = datetime.now(tz=timezone.utc), age: timedelta | None = None
) -> Sequence[Mapping[str, Any]]:
    """
    Pipeline to generate a balance sheet as of a specific date.

    Args:
        as_of_date (datetime): The date for which the balance sheet is generated.

    Returns:
        dict: A dictionary representing the balance sheet.
    """
    # Fetch ledger entries
    if age:
        start_date = as_of_date - age
        date_range_query = {"$gte": start_date, "$lte": as_of_date}
    else:
        date_range_query = {"$lte": as_of_date}

    pipeline: Sequence[Mapping[str, Any]] = [
        {"$match": {"timestamp": date_range_query, "conv_signed": {"$exists": True}}},
        {
            "$match": {
                "$or": [
                    {"debit.account_type": {"$in": ["Asset", "Liability"]}},
                    {"credit.account_type": {"$in": ["Asset", "Liability"]}},
                ]
            }
        },
        {
            "$facet": {
                "debit_sums": [
                    {
                        "$group": {
                            "_id": {
                                "name": "$debit.name",
                                "sub": "$debit.sub",
                                "type": "$debit.account_type",
                            },
                            "count": {"$sum": 1},
                            "hbd": {"$sum": "$conv_signed.debit.hbd"},
                            "hive": {"$sum": "$conv_signed.debit.hive"},
                            "msats": {"$sum": "$conv_signed.debit.msats"},
                            "sats": {"$sum": "$conv_signed.debit.sats"},
                            "usd": {"$sum": "$conv_signed.debit.usd"},
                        }
                    }
                ],
                "credit_sums": [
                    {
                        "$group": {
                            "_id": {
                                "name": "$credit.name",
                                "sub": "$credit.sub",
                                "type": "$credit.account_type",
                            },
                            "count": {"$sum": 1},
                            "hbd": {"$sum": "$conv_signed.credit.hbd"},
                            "hive": {"$sum": "$conv_signed.credit.hive"},
                            "msats": {"$sum": "$conv_signed.credit.msats"},
                            "sats": {"$sum": "$conv_signed.credit.sats"},
                            "usd": {"$sum": "$conv_signed.credit.usd"},
                        }
                    }
                ],
            }
        },
        {"$project": {"all": {"$concatArrays": ["$debit_sums", "$credit_sums"]}}},
        {"$unwind": "$all"},
        {
            "$group": {
                "_id": "$all._id",
                "count": {"$sum": "$all.count"},
                "hbd": {"$sum": "$all.hbd"},
                "hive": {"$sum": "$all.hive"},
                "msats": {"$sum": "$all.msats"},
                "sats": {"$sum": "$all.sats"},
                "usd": {"$sum": "$all.usd"},
            }
        },
        {"$sort": {"_id.type": 1, "_id.name": 1, "_id.sub": 1}},
        {
            "$facet": {
                "assets": [
                    {"$match": {"_id.type": "Asset"}},
                    {
                        "$group": {
                            "_id": "$_id.name",
                            "subs": {
                                "$push": {
                                    "k": "$_id.sub",
                                    "v": {
                                        "count": "$count",
                                        "hbd": "$hbd",
                                        "hive": "$hive",
                                        "msats": "$msats",
                                        "sats": "$sats",
                                        "usd": "$usd",
                                    },
                                }
                            },
                            "total_count": {"$sum": "$count"},
                            "total_hbd": {"$sum": "$hbd"},
                            "total_hive": {"$sum": "$hive"},
                            "total_msats": {"$sum": "$msats"},
                            "total_sats": {"$sum": "$sats"},
                            "total_usd": {"$sum": "$usd"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "k": "$_id",
                            "v": {
                                "$mergeObjects": [
                                    {
                                        "Total": {
                                            "count": "$total_count",
                                            "hbd": "$total_hbd",
                                            "hive": "$total_hive",
                                            "msats": "$total_msats",
                                            "sats": "$total_sats",
                                            "usd": "$total_usd",
                                        }
                                    },
                                    {"$arrayToObject": "$subs"},
                                ]
                            },
                        }
                    },
                    {"$sort": {"k": 1}},
                ],
                "liabilities": [
                    {"$match": {"_id.type": "Liability"}},
                    {
                        "$group": {
                            "_id": "$_id.name",
                            "subs": {
                                "$push": {
                                    "k": "$_id.sub",
                                    "v": {
                                        "count": "$count",
                                        "hbd": "$hbd",
                                        "hive": "$hive",
                                        "msats": "$msats",
                                        "sats": "$sats",
                                        "usd": "$usd",
                                    },
                                }
                            },
                            "total_count": {"$sum": "$count"},
                            "total_hbd": {"$sum": "$hbd"},
                            "total_hive": {"$sum": "$hive"},
                            "total_msats": {"$sum": "$msats"},
                            "total_sats": {"$sum": "$sats"},
                            "total_usd": {"$sum": "$usd"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "k": "$_id",
                            "v": {
                                "$mergeObjects": [
                                    {
                                        "Total": {
                                            "count": "$total_count",
                                            "hbd": "$total_hbd",
                                            "hive": "$total_hive",
                                            "msats": "$total_msats",
                                            "sats": "$total_sats",
                                            "usd": "$total_usd",
                                        }
                                    },
                                    {"$arrayToObject": "$subs"},
                                ]
                            },
                        }
                    },
                    {"$sort": {"k": 1}},
                ],
            }
        },
        {
            "$project": {
                "Assets": {"$arrayToObject": "$assets"},
                "Liabilities": {"$arrayToObject": "$liabilities"},
                "total_assets_hbd": {"$sum": "$assets.v.Total.hbd"},
                "total_assets_hive": {"$sum": "$assets.v.Total.hive"},
                "total_assets_msats": {"$sum": "$assets.v.Total.msats"},
                "total_assets_sats": {"$sum": "$assets.v.Total.sats"},
                "total_assets_usd": {"$sum": "$assets.v.Total.usd"},
                "total_liabilities_hbd": {"$sum": "$liabilities.v.Total.hbd"},
                "total_liabilities_hive": {"$sum": "$liabilities.v.Total.hive"},
                "total_liabilities_msats": {"$sum": "$liabilities.v.Total.msats"},
                "total_liabilities_sats": {"$sum": "$liabilities.v.Total.sats"},
                "total_liabilities_usd": {"$sum": "$liabilities.v.Total.usd"},
            }
        },
    ]
    return pipeline


def profit_loss_pipeline(
    as_of_date: datetime | None = None, age: timedelta | None = None
) -> Sequence[Mapping[str, Any]]:
    """
    Pipeline to generate a profit and loss statement as of a specific date.

    Args:
        as_of_date (datetime): The date for which the profit and loss statement is generated.

    Returns:
        dict: A dictionary representing the profit and loss statement.
    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)

    if age:
        start_date = as_of_date - age
        date_range_query = {"$gte": start_date, "$lte": as_of_date}
    else:
        date_range_query = {"$lte": as_of_date}

    pipeline: Sequence[Mapping[str, Any]] = [
        {"$match": {"timestamp": date_range_query, "conv_signed": {"$exists": True}}},
        {
            "$facet": {
                "revenue_details": [
                    {"$match": {"credit.account_type": "Revenue"}},
                    {
                        "$group": {
                            "_id": {"name": "$credit.name", "sub": "$credit.sub"},
                            "hbd": {"$sum": "$conv_signed.credit.hbd"},
                            "hive": {"$sum": "$conv_signed.credit.hive"},
                            "msats": {"$sum": "$conv_signed.credit.msats"},
                            "sats": {"$sum": "$conv_signed.credit.sats"},
                            "usd": {"$sum": "$conv_signed.credit.usd"},
                        }
                    },
                    {"$sort": {"_id.name": 1, "_id.sub": 1}},
                ],
                "expense_details": [
                    {"$match": {"debit.account_type": "Expense"}},
                    {
                        "$group": {
                            "_id": {"name": "$debit.name", "sub": "$debit.sub"},
                            "hbd": {"$sum": "$conv_signed.debit.hbd"},
                            "hive": {"$sum": "$conv_signed.debit.hive"},
                            "msats": {"$sum": "$conv_signed.debit.msats"},
                            "sats": {"$sum": "$conv_signed.debit.sats"},
                            "usd": {"$sum": "$conv_signed.debit.usd"},
                        }
                    },
                    {"$sort": {"_id.name": 1, "_id.sub": 1}},
                ],
            }
        },
        {
            "$project": {
                "Revenue": {
                    "$map": {
                        "input": {
                            "$setUnion": [
                                {
                                    "$map": {
                                        "input": "$revenue_details",
                                        "as": "r",
                                        "in": "$$r._id.name",
                                    }
                                }
                            ]
                        },
                        "as": "name",
                        "in": {
                            "k": "$$name",
                            "v": {
                                "$mergeObjects": [
                                    {
                                        "Total": {
                                            "hbd": {
                                                "$sum": {
                                                    "$map": {
                                                        "input": {
                                                            "$filter": {
                                                                "input": "$revenue_details",
                                                                "as": "r",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$r._id.name",
                                                                        "$$name",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        "as": "f",
                                                        "in": "$$f.hbd",
                                                    }
                                                }
                                            },
                                            "hive": {
                                                "$sum": {
                                                    "$map": {
                                                        "input": {
                                                            "$filter": {
                                                                "input": "$revenue_details",
                                                                "as": "r",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$r._id.name",
                                                                        "$$name",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        "as": "f",
                                                        "in": "$$f.hive",
                                                    }
                                                }
                                            },
                                            "msats": {
                                                "$sum": {
                                                    "$map": {
                                                        "input": {
                                                            "$filter": {
                                                                "input": "$revenue_details",
                                                                "as": "r",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$r._id.name",
                                                                        "$$name",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        "as": "f",
                                                        "in": "$$f.msats",
                                                    }
                                                }
                                            },
                                            "sats": {
                                                "$sum": {
                                                    "$map": {
                                                        "input": {
                                                            "$filter": {
                                                                "input": "$revenue_details",
                                                                "as": "r",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$r._id.name",
                                                                        "$$name",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        "as": "f",
                                                        "in": "$$f.sats",
                                                    }
                                                }
                                            },
                                            "usd": {
                                                "$sum": {
                                                    "$map": {
                                                        "input": {
                                                            "$filter": {
                                                                "input": "$revenue_details",
                                                                "as": "r",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$r._id.name",
                                                                        "$$name",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        "as": "f",
                                                        "in": "$$f.usd",
                                                    }
                                                }
                                            },
                                        }
                                    },
                                    {
                                        "$arrayToObject": {
                                            "$map": {
                                                "input": {
                                                    "$filter": {
                                                        "input": "$revenue_details",
                                                        "as": "r",
                                                        "cond": {
                                                            "$eq": ["$$r._id.name", "$$name"]
                                                        },
                                                    }
                                                },
                                                "as": "item",
                                                "in": {
                                                    "k": "$$item._id.sub",
                                                    "v": {
                                                        "hbd": "$$item.hbd",
                                                        "hive": "$$item.hive",
                                                        "msats": "$$item.msats",
                                                        "sats": "$$item.sats",
                                                        "usd": "$$item.usd",
                                                    },
                                                },
                                            }
                                        }
                                    },
                                ]
                            },
                        },
                    }
                },
                "Expenses": {
                    "$map": {
                        "input": {
                            "$setUnion": [
                                {
                                    "$map": {
                                        "input": "$expense_details",
                                        "as": "e",
                                        "in": "$$e._id.name",
                                    }
                                }
                            ]
                        },
                        "as": "name",
                        "in": {
                            "k": "$$name",
                            "v": {
                                "$mergeObjects": [
                                    {
                                        "Total": {
                                            "hbd": {
                                                "$sum": {
                                                    "$map": {
                                                        "input": {
                                                            "$filter": {
                                                                "input": "$expense_details",
                                                                "as": "e",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$e._id.name",
                                                                        "$$name",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        "as": "f",
                                                        "in": "$$f.hbd",
                                                    }
                                                }
                                            },
                                            "hive": {
                                                "$sum": {
                                                    "$map": {
                                                        "input": {
                                                            "$filter": {
                                                                "input": "$expense_details",
                                                                "as": "e",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$e._id.name",
                                                                        "$$name",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        "as": "f",
                                                        "in": "$$f.hive",
                                                    }
                                                }
                                            },
                                            "msats": {
                                                "$sum": {
                                                    "$map": {
                                                        "input": {
                                                            "$filter": {
                                                                "input": "$expense_details",
                                                                "as": "e",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$e._id.name",
                                                                        "$$name",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        "as": "f",
                                                        "in": "$$f.msats",
                                                    }
                                                }
                                            },
                                            "sats": {
                                                "$sum": {
                                                    "$map": {
                                                        "input": {
                                                            "$filter": {
                                                                "input": "$expense_details",
                                                                "as": "e",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$e._id.name",
                                                                        "$$name",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        "as": "f",
                                                        "in": "$$f.sats",
                                                    }
                                                }
                                            },
                                            "usd": {
                                                "$sum": {
                                                    "$map": {
                                                        "input": {
                                                            "$filter": {
                                                                "input": "$expense_details",
                                                                "as": "e",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$e._id.name",
                                                                        "$$name",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        "as": "f",
                                                        "in": "$$f.usd",
                                                    }
                                                }
                                            },
                                        }
                                    },
                                    {
                                        "$arrayToObject": {
                                            "$map": {
                                                "input": {
                                                    "$filter": {
                                                        "input": "$expense_details",
                                                        "as": "e",
                                                        "cond": {
                                                            "$eq": ["$$e._id.name", "$$name"]
                                                        },
                                                    }
                                                },
                                                "as": "item",
                                                "in": {
                                                    "k": "$$item._id.sub",
                                                    "v": {
                                                        "hbd": "$$item.hbd",
                                                        "hive": "$$item.hive",
                                                        "msats": "$$item.msats",
                                                        "sats": "$$item.sats",
                                                        "usd": "$$item.usd",
                                                    },
                                                },
                                            }
                                        }
                                    },
                                ]
                            },
                        },
                    }
                },
                "Net Income": {
                    "$mergeObjects": [
                        {
                            "Total": {
                                "hbd": {
                                    "$subtract": [
                                        {"$sum": "$revenue_details.hbd"},
                                        {"$sum": "$expense_details.hbd"},
                                    ]
                                },
                                "hive": {
                                    "$subtract": [
                                        {"$sum": "$revenue_details.hive"},
                                        {"$sum": "$expense_details.hive"},
                                    ]
                                },
                                "msats": {
                                    "$subtract": [
                                        {"$sum": "$revenue_details.msats"},
                                        {"$sum": "$expense_details.msats"},
                                    ]
                                },
                                "sats": {
                                    "$subtract": [
                                        {"$sum": "$revenue_details.sats"},
                                        {"$sum": "$expense_details.sats"},
                                    ]
                                },
                                "usd": {
                                    "$subtract": [
                                        {"$sum": "$revenue_details.usd"},
                                        {"$sum": "$expense_details.usd"},
                                    ]
                                },
                            }
                        },
                        {
                            "$arrayToObject": {
                                "$map": {
                                    "input": {
                                        "$setUnion": [
                                            {
                                                "$map": {
                                                    "input": "$revenue_details",
                                                    "as": "r",
                                                    "in": "$$r._id.sub",
                                                }
                                            },
                                            {
                                                "$map": {
                                                    "input": "$expense_details",
                                                    "as": "e",
                                                    "in": "$$e._id.sub",
                                                }
                                            },
                                        ]
                                    },
                                    "as": "sub",
                                    "in": {
                                        "k": "$$sub",
                                        "v": {
                                            "hbd": {
                                                "$subtract": [
                                                    {
                                                        "$sum": {
                                                            "$map": {
                                                                "input": {
                                                                    "$filter": {
                                                                        "input": "$revenue_details",
                                                                        "as": "r",
                                                                        "cond": {
                                                                            "$eq": [
                                                                                "$$r._id.sub",
                                                                                "$$sub",
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                "as": "f",
                                                                "in": "$$f.hbd",
                                                            }
                                                        }
                                                    },
                                                    {
                                                        "$sum": {
                                                            "$map": {
                                                                "input": {
                                                                    "$filter": {
                                                                        "input": "$expense_details",
                                                                        "as": "e",
                                                                        "cond": {
                                                                            "$eq": [
                                                                                "$$e._id.sub",
                                                                                "$$sub",
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                "as": "f",
                                                                "in": "$$f.hbd",
                                                            }
                                                        }
                                                    },
                                                ]
                                            },
                                            "hive": {
                                                "$subtract": [
                                                    {
                                                        "$sum": {
                                                            "$map": {
                                                                "input": {
                                                                    "$filter": {
                                                                        "input": "$revenue_details",
                                                                        "as": "r",
                                                                        "cond": {
                                                                            "$eq": [
                                                                                "$$r._id.sub",
                                                                                "$$sub",
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                "as": "f",
                                                                "in": "$$f.hive",
                                                            }
                                                        }
                                                    },
                                                    {
                                                        "$sum": {
                                                            "$map": {
                                                                "input": {
                                                                    "$filter": {
                                                                        "input": "$expense_details",
                                                                        "as": "e",
                                                                        "cond": {
                                                                            "$eq": [
                                                                                "$$e._id.sub",
                                                                                "$$sub",
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                "as": "f",
                                                                "in": "$$f.hive",
                                                            }
                                                        }
                                                    },
                                                ]
                                            },
                                            "msats": {
                                                "$subtract": [
                                                    {
                                                        "$sum": {
                                                            "$map": {
                                                                "input": {
                                                                    "$filter": {
                                                                        "input": "$revenue_details",
                                                                        "as": "r",
                                                                        "cond": {
                                                                            "$eq": [
                                                                                "$$r._id.sub",
                                                                                "$$sub",
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                "as": "f",
                                                                "in": "$$f.msats",
                                                            }
                                                        }
                                                    },
                                                    {
                                                        "$sum": {
                                                            "$map": {
                                                                "input": {
                                                                    "$filter": {
                                                                        "input": "$expense_details",
                                                                        "as": "e",
                                                                        "cond": {
                                                                            "$eq": [
                                                                                "$$e._id.sub",
                                                                                "$$sub",
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                "as": "f",
                                                                "in": "$$f.msats",
                                                            }
                                                        }
                                                    },
                                                ]
                                            },
                                            "sats": {
                                                "$subtract": [
                                                    {
                                                        "$sum": {
                                                            "$map": {
                                                                "input": {
                                                                    "$filter": {
                                                                        "input": "$revenue_details",
                                                                        "as": "r",
                                                                        "cond": {
                                                                            "$eq": [
                                                                                "$$r._id.sub",
                                                                                "$$sub",
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                "as": "f",
                                                                "in": "$$f.sats",
                                                            }
                                                        }
                                                    },
                                                    {
                                                        "$sum": {
                                                            "$map": {
                                                                "input": {
                                                                    "$filter": {
                                                                        "input": "$expense_details",
                                                                        "as": "e",
                                                                        "cond": {
                                                                            "$eq": [
                                                                                "$$e._id.sub",
                                                                                "$$sub",
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                "as": "f",
                                                                "in": "$$f.sats",
                                                            }
                                                        }
                                                    },
                                                ]
                                            },
                                            "usd": {
                                                "$subtract": [
                                                    {
                                                        "$sum": {
                                                            "$map": {
                                                                "input": {
                                                                    "$filter": {
                                                                        "input": "$revenue_details",
                                                                        "as": "r",
                                                                        "cond": {
                                                                            "$eq": [
                                                                                "$$r._id.sub",
                                                                                "$$sub",
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                "as": "f",
                                                                "in": "$$f.usd",
                                                            }
                                                        }
                                                    },
                                                    {
                                                        "$sum": {
                                                            "$map": {
                                                                "input": {
                                                                    "$filter": {
                                                                        "input": "$expense_details",
                                                                        "as": "e",
                                                                        "cond": {
                                                                            "$eq": [
                                                                                "$$e._id.sub",
                                                                                "$$sub",
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                "as": "f",
                                                                "in": "$$f.usd",
                                                            }
                                                        }
                                                    },
                                                ]
                                            },
                                        },
                                    },
                                }
                            }
                        },
                    ]
                },
            }
        },
        {
            "$project": {
                "Expenses": {"$arrayToObject": "$Expenses"},
                "Net Income": "$Net Income",
                "Revenue": {"$arrayToObject": "$Revenue"},
            }
        },
    ]
    return pipeline
