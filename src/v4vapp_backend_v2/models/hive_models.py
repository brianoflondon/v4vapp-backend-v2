from datetime import datetime, timezone
from timeit import default_timer as timer
from typing import Any, Dict

from beem.amount import Amount  # type: ignore
from colorama import Fore
from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    detect_convert_keepsats,
    detect_keepsats,
    detect_paywithsats,
)
from v4vapp_backend_v2.helpers.hive_extras import (
    decode_memo,
    get_blockchain_instance,
    get_event_id,
    get_hive_client,
)


class HiveTransactionFlags(BaseModel):
    encrypted: bool = False
    decrypted: bool = False
    processed: bool = False
    answer_trx_id: str = ""
    hive_result: dict = {}
    pay_result: dict = {}
    streaming: bool = False
    success: bool = False
    confirmed: bool = False
    in_transition: bool = False
    keep_sats: bool = False
    pay_with_keep_sats: bool = False
    fee_sats: float = 0.0
    force_send_sats: int = 0


class HiveTransaction(BaseModel):
    id: str = Field(..., alias="_id")
    trx_id: str
    timestamp: datetime
    found: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    op_in_trx: int
    hive_from: str = Field(..., alias="from")
    hive_to: str = Field(..., alias="to")
    amount: dict = {}
    amount_str: str = ""
    amount_decimal: str = ""
    amount_symbol: str = ""
    amount_value: float = 0.0
    conv: CryptoConv = CryptoConv()
    memo: str
    d_memo: str = ""
    d_memo_extra: str = ""
    block_num: int

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **post: Any) -> None:
        post["_id"] = get_event_id(post)
        super().__init__(**post)
        amount = Amount(self.amount)
        self.amount_str = str(amount)
        self.amount_decimal = str(amount.amount_decimal)
        self.amount_symbol = amount.symbol
        self.amount_value = amount.amount

        hive_inst = post.get("hive_inst", None)
        if "hive_inst" in post:
            self.d_memo = decode_memo(hive_inst=hive_inst, memo=self.memo)
        elif "memo_keys" in post:
            self.d_memo = decode_memo(memo_keys=post["memo_keys"], memo=self.memo)
        else:
            self.d_memo = self.memo

    @property
    def encrypted(self) -> bool:
        return self.d_memo != self.memo


if __name__ == "__main__":
    internal_config = InternalConfig()
    hive_config = internal_config.config.hive
    memo_keys = hive_config.memo_keys
    name_memo_keys = hive_config.name_memo_keys
    hive = get_hive_client(keys=memo_keys)
    # get_current_block_num = hive.get_dynamic_global_properties().get(
    #     "head_block_number"
    # )
    get_current_block_num = 94094293
    blockchain = get_blockchain_instance(hive_instance=hive)
    op_in_trx = 0
    last_trx_id = ""

    all_posts = []
    try:
        for post in blockchain.stream(
            opNames=["transfer", "recurrent_transfer"],
            start=get_current_block_num - 250,
            stop=get_current_block_num + 750,
            max_batch_size=20,
        ):

            if last_trx_id == post["trx_id"]:
                op_in_trx += 1
            else:
                op_in_trx = 0
                last_trx_id = post["trx_id"]
            post["op_in_trx"] = op_in_trx
            all_posts.append(post)

    except TypeError as e:
        logger.info(f"Error: {e}")

    start = timer()
    for post in all_posts:
        hive_trx = HiveTransaction(**post, hive_inst=hive)

        if hive_trx.encrypted:
            logger.info(f"{Fore.YELLOW}{hive_trx.id} {hive_trx.d_memo}")
        else:
            logger.info(f"{hive_trx.id} {hive_trx.d_memo}")

    end = timer()
    logger.info(f"Time taken: {end - start}")
