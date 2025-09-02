from typing import Dict, List

from pydantic import BaseModel

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry


class PeriodResult(BaseModel):
    hbd: float
    hive: float
    msats: int
    sats: int
    usd: float
    limit_sats: int | None = None
    limit_ok: bool = False
    details: List[LedgerEntry] | None = None

    def limit_text(self, period: str, cust_id: str) -> str:
        ok_str = "ok" if self.limit_ok else "exceeded"
        if self.limit_sats is not None:
            return f"Lightning conversions for {cust_id} in the last {period} hours: {self.sats:,.0f} sats (limit: {self.limit_sats:,.0f} sats, {ok_str})"
        else:
            return f"Lightning conversions for {cust_id} in the last {period} hours: {self.sats:,.0f} sats"


class LimitCheckResult(BaseModel):
    cust_id: str
    periods: Dict[str, PeriodResult]

    def __init__(self, **data):
        super().__init__(**data)

    def __str__(self):
        lines = [f"Limit Check for Customer ID: {self.cust_id}"]
        for period, result in self.periods.items():
            lines.append(f"  Period: {period}")
            lines.append(f"    Sats: {result.sats}")
            lines.append(f"    Msats: {result.msats}")
            lines.append(f"    USD: {result.usd:.2f}")
            lines.append(f"    HBD: {result.hbd:.2f}")
            lines.append(f"    Hive: {result.hive:.2f}")
            lines.append(f"    Limit Sats: {result.limit_sats}")
            lines.append(result.limit_text(period, self.cust_id))
            if result.details:
                lines.append(f"    Details ({len(result.details)} entries):")
                for entry in result.details:
                    lines.append(f"      - {entry}")
        return "\n".join(lines)

    def limit_text(self) -> str:
        lines = [f"Limit Check Summary for Customer ID: {self.cust_id}"]
        for period, result in self.periods.items():
            lines.append(f"  {result.limit_text(period, self.cust_id)}")
        return "\n".join(lines)
