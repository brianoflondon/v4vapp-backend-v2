# import re
# from typing import Any

# from v4vapp_backend_v2.models.invoice_models import Invoice

# from v4vapp_backend_v2.config.setup import LoggerFunction

# # This is the regex for finding if a given message is an LND invoice to pay.
# # This looks for #v4vapp v4vapp
# LND_INVOICE_TAG = r"(.*)(#(v4vapp))"


# class LNDInvoice(Invoice):

#     is_lndtohive: bool = False

#     def __init__(__pydantic_self__, **data: Any) -> None:
#         super().__init__(**data)
#         # perform my check to see if this invoice can be paid to Hive
#         if __pydantic_self__.memo:
#             match = re.match(LND_INVOICE_TAG, __pydantic_self__.memo.lower())
#             if match:
#                 __pydantic_self__.is_lndtohive = True

#     def invoice_message(self) -> str:
#         if self.settled:
#             return (
#                 f"✅ Settled invoice {self.add_index} "
#                 f"with memo {self.memo} {self.value:,.0f} sats"
#             )
#         else:
#             return (
#                 f"✅ Valid   invoice {self.add_index} "
#                 f"with memo {self.memo} {self.value:,.0f} sats"
#             )

#     def invoice_log(
#         self, logger_func: LoggerFunction, send_notification: bool = False
#     ) -> None:
#         logger_func(
#             self.invoice_message(),
#             extra={
#                 "notification": send_notification,
#                 "invoice": self.model_dump(exclude_none=True, exclude_unset=True),
#             },
#         )


# class LNDInvoice(BaseModel):
#     """Model of an LND Invoice"""

#     memo: str = ""
#     r_preimage: str | None = None
#     r_hash: str
#     value: int = 0
#     value_msat: int = 0
#     settled: bool = False
#     creation_date: datetime
#     settle_date: datetime | None = None
#     payment_request: str | None = None
#     description_hash: str | None = None
#     expiry: int | None = None
#     fallback_addr: str | None = None
#     cltv_expiry: int
#     route_hints: List[dict] | None = None
#     private: bool | None = None
#     add_index: int = 0
#     settle_index: int = 0
#     amt_paid: int | None = None
#     amt_paid_sat: int | None = None
#     amt_paid_msat: int | None = None
#     state: str | None = None
#     htlcs: List[dict] | None = None
#     features: dict
#     is_keysend: bool = False
#     payment_addr: str
#     is_amp: bool = False
#     is_lndtohive: bool = False

#     def __init__(__pydantic_self__, **data: Any) -> None:
#         super().__init__(**data)
#         # perform my check to see if this invoice can be paid to Hive
#         if __pydantic_self__.memo:
#             match = re.match(LND_INVOICE_TAG, __pydantic_self__.memo.lower())
#             if match:
#                 __pydantic_self__.is_lndtohive = True

#     def invoice_message(self) -> str:
#         if self.settled:
#             return (
#                 f"✅ Settled invoice {self.add_index} "
#                 f"with memo {self.memo} {self.value:,.0f} sats"
#             )
#         else:
#             return (
#                 f"✅ Valid   invoice {self.add_index} "
#                 f"with memo {self.memo} {self.value:,.0f} sats"
#             )

#     def invoice_log(
#         self, logger_func: LoggerFunction, send_notification: bool = False
#     ) -> None:
#         logger_func(
#             self.invoice_message(),
#             extra={
#                 "notification": send_notification,
#                 "invoice": self.model_dump(exclude_none=True),
#             },
#         )
