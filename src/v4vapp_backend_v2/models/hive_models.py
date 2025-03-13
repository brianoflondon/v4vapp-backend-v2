from datetime import datetime
from typing import Any

from beem.amount import Amount  # type: ignore
from pydantic import BaseModel

from v4vapp_backend_v2.config.setup import InternalConfig, logger


class HivePostTransfer(BaseModel):
    hive_from: str
    hive_to: str
    amount: dict
    memo: str
    d_memo: str = None
    d_memo_extra: str = None
    lnurl: bool = False
    timestamp: datetime
    block_num: int
    trx_id: str
    encrypted: bool = False
    decrypted: bool = False
    value: dict = {}
    processed: bool = False
    sats: float = None
    found: datetime = None
    answer_trx_id: str = None
    hive_result: dict = None
    pay_result: dict = None
    hive_equiv: float = None
    USD_equiv: float = None
    HBD_equiv: float = None
    sats_Hive: float = None
    streaming: bool = False
    success: bool = False
    confirmed: bool = False
    in_transition: bool = False
    keep_sats: bool = False
    pay_with_keep_sats: bool = False
    convert_keep_sats: bool = False
    convert_keep_sats_amount: int = None
    convert_keep_sats_account: str = None
    fee_sats: float = None
    force_send_sats: int = None

    class Config:
        fields = {"hive_from": "from", "hive_to": "to"}
        allow_population_by_field_name = True
        arbitrary_types_allowed = True

    def __init__(__pydantic_self__, **post: Any) -> None:
        super().__init__(**post)
        amt = Amount(post["amount"])

        __pydantic_self__.value["amount"] = amt.amount
        __pydantic_self__.value["symbol"] = amt.symbol
        __pydantic_self__.value["amount_decimal"] = str(amt.amount_decimal)

        if post["memo"][:5] == "#lnbc":
            post["memo"] = post["memo"][1:]

        if post["memo"][:1] == "#":
            __pydantic_self__.encrypted = True
        else:
            __pydantic_self__.d_memo = post["memo"]

        if detect_paywithsats(__pydantic_self__.d_memo):
            __pydantic_self__.pay_with_keep_sats = True

        if detect_keepsats(__pydantic_self__.d_memo):
            __pydantic_self__.keep_sats = True

        if detect_convert_keepsats(__pydantic_self__.d_memo):
            """
            Needs to take the form `<amount> [@<account>] #convertkeepsats`

            The amount is in sats and can have , but not .
            If the account is not given, it will be assumed to be the account sending
            the request.
            """
            pattern = r"([\d,]+(\.\d+)?)? ?(@\w+)? ?#convertkeepsats\s.*"
            match = re.search(pattern, __pydantic_self__.d_memo.lower())
            if match:
                __pydantic_self__.convert_keep_sats_amount = int(
                    int(match.group(1).replace(",", "")) if match.group(1) else None
                )
                __pydantic_self__.convert_keep_sats_account = (
                    match.group(3) if match.group(3) else __pydantic_self__.hive_from
                )
                if (
                    __pydantic_self__.convert_keep_sats_amount
                    and __pydantic_self__.convert_keep_sats_account
                ):
                    __pydantic_self__.convert_keep_sats = True

    def decode_memo(
        self, memo_keys: Optional[List[str]] = None, hive_inst: Optional[Hive] = None
    ) -> str:
        # Decode encrypted memo
        # found problem with Beem deocding some memos, can't sort this out.
        try:
            if self.encrypted:
                if hive_inst:
                    hive = hive_inst
                else:
                    hive = Hive(keys=[memo_keys])

                m = Memo(
                    from_account=self.hive_from,
                    to_account=self.hive_to,
                    blockchain_instance=hive,
                )
                self.d_memo = m.decrypt(self.memo)[1:]
                self.decrypted = True
            else:
                self.d_memo = self.memo
            return self.d_memo
        except struct.error as e:
            # Likely caused by receiving a memo which starts with "#" but isn't encrypted.
            logger.warning(f"Memo not encrypted: {e}")
            logger.warning(self.memo)
            self.d_memo = self.memo
            return self.d_memo
        except Exception as e:
            logger.error(f"Some problem in decode_memo: {e}")
            logger.error(self.memo)
            logger.exception(e)
            self.d_memo = self.memo
            return self.d_memo

    def symbol_value(self) -> dict:
        """Return a dict to conver into CryptoConversion object"""
        return {self.value["symbol"]: self.value["amount"]}

    @property
    def amount_str(self) -> str:
        """Return the amount in the value dict as a string"""
        return f"{self.value.get('amount',0):,.3f} {self.value.get('symbol','')}"
