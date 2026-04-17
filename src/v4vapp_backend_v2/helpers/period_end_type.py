import calendar
from datetime import date, datetime, timedelta, timezone
from enum import StrEnum
from typing import List


class PeriodType(StrEnum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


# ---------------------------------------------------------------------------
# Period boundary helpers
# ---------------------------------------------------------------------------


def period_end_for_date(period_type: PeriodType, d: date) -> datetime:
    """Return the last microsecond of the calendar period that contains *d*.

    The returned datetime is timezone-aware (UTC) and represents the inclusive
    upper bound: all transactions with ``timestamp ≤ period_end`` belong to
    this period's checkpoint.
    """
    if period_type == PeriodType.DAILY:
        return datetime(d.year, d.month, d.day, 23, 59, 59, 999999, tzinfo=timezone.utc)

    if period_type == PeriodType.WEEKLY:
        # ISO week: Monday=0 … Sunday=6; advance to Sunday
        days_to_sunday = 6 - d.weekday()
        sunday = d + timedelta(days=days_to_sunday)
        return datetime(
            sunday.year, sunday.month, sunday.day, 23, 59, 59, 999999, tzinfo=timezone.utc
        )

    if period_type == PeriodType.MONTHLY:
        last_day = calendar.monthrange(d.year, d.month)[1]
        return datetime(d.year, d.month, last_day, 23, 59, 59, 999999, tzinfo=timezone.utc)

    raise ValueError(f"Unknown period type: {period_type}")


def last_completed_period_end(period_type: PeriodType, now: datetime | None = None) -> datetime:
    """Return the end datetime of the last fully completed period before *now*.

    Unlike :func:`period_end_for_date`, which returns the end of the period
    *containing* a given date (which may be in the future), this function
    always returns a period boundary that has already passed:

    - **daily**  → end of yesterday
    - **weekly** → end of the most-recently completed ISO week (last Sunday)
    - **monthly** → end of the previous calendar month
    """
    if now is None:
        now = datetime.now(tz=timezone.utc)
    today = now.date()

    if period_type == PeriodType.DAILY:
        d = today - timedelta(days=1)
    elif period_type == PeriodType.WEEKLY:
        # weekday(): Mon=0 … Sun=6; go back enough days to land on the last Sunday
        days_since_last_sunday = today.weekday() + 1  # Mon→1, Tue→2, …, Sun→7
        d = today - timedelta(days=days_since_last_sunday)
    elif period_type == PeriodType.MONTHLY:
        # First day of this month minus one day = last day of previous month
        d = date(today.year, today.month, 1) - timedelta(days=1)
    else:
        raise ValueError(f"Unknown period type: {period_type}")

    return period_end_for_date(period_type, d)


def completed_period_ends_since(
    period_type: PeriodType,
    since: datetime,
    until: datetime,
) -> List[datetime]:
    """Return a list of completed period-end timestamps in chronological order.

    Only periods whose ``period_end`` is strictly less than *until* are
    included, so the caller's "current" period is never returned as complete.

    Args:
        period_type: Granularity of the periods to enumerate.
        since: Start of the range (inclusive).
        until: End of the range (exclusive).  Typically *now*.
    """
    results: List[datetime] = []
    current = since.date()
    while True:
        end = period_end_for_date(period_type, current)
        if end >= until:
            break
        if end > since:
            results.append(end)

        # Advance to the next period start
        if period_type == PeriodType.DAILY:
            current = current + timedelta(days=1)
        elif period_type == PeriodType.WEEKLY:
            days_to_next_monday = 7 - current.weekday()
            current = current + timedelta(days=days_to_next_monday)
        elif period_type == PeriodType.MONTHLY:
            last_day = calendar.monthrange(current.year, current.month)[1]
            first_next = date(current.year, current.month, last_day) + timedelta(days=1)
            current = first_next

    return results
