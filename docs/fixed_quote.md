This is the code from the API we need to support which is used to call for a fixed quote for a Hive invoice. It handles different currencies and fetches the necessary conversion rates.


From the api external code:

```python
    def __init__(__pydantic_self__, **data: Any) -> None:
        url = INTERNAL_API_SERVER_URL + "cryptoprices/fixed_quote/"
        currency_conversion: LookupCurrency = LookupCurrency.HIVE
        if hive_amount := data.get("hive_amount"):
            url += f"?HIVE={hive_amount}"
            currency_conversion = LookupCurrency.HIVE
        elif hbd_amount := data.get("hbd_amount"):
            url += f"?HBD={hbd_amount}"
            currency_conversion = LookupCurrency.HBD
        elif usd_amount := data.get("usd_amount"):
            url += f"?USD={usd_amount}"
            currency_conversion = LookupCurrency.USD
        elif sats_amount := data.get("sats_amount"):
            # Going to keep the sats amount in sats
            url += f"?SATS={usd_amount}"
            sats_amount = int(sats_amount)
            data["amount"] = sats_amount
            data["message"] = f"{data['message']} | #SATS {sats_amount}"
            data["unique_id"] = uuid.uuid4()
            currency_conversion = LookupCurrency.SATS

        if not currency_conversion == LookupCurrency.SATS:
            url += f"&cache_time={data.get('expiry', 300)}"
            logging.info(f"Calling {url}")
            r = httpx.get(url=url, timeout=5)
            if not is_status_code_success(r.status_code):
                logging.error("Bad response getting rates")
                logging.error(r.text)
                raise HTTPException(status_code=r.status_code, detail=r.text)
            answer = r.json()
            data["amount"] = answer.get("sats_send")
            data["unique_id"] = answer.get("unique_id")
            data["message"] = f"{data['message']} | #UUID {answer.get('unique_id')}"

        if data.get("receive_currency") == LnurlCurrencyEnum.hbd or data.get("hbd"):
            data["message"] = f"{data['message']} | #HBD"
        if data.get("receive_currency") == LnurlCurrencyEnum.sats:
            data["message"] = f"{data['message']} | #SATS"

        try:
            super().__init__(**data)
        except ValidationError as e:
            logging.error(e)
            raise e
        logging.info(
            f"New Invoice for Hive App_name: {__pydantic_self__.app_name} | "
            f"{__pydantic_self__.amount} sats | "
            f"{__pydantic_self__.message}"
        )

```

The reply from the API looks like this;

```json
{
    "unique_id": "6f5a0749-fec3-42b7-a004-6cddcaaef81c",
    "sats_send": 9541,
    "conv": {
        "conv_from": "HIVE",
        "sats": 9326.912,
        "HIVE": 47.518862,
        "HBD": 11.406711,
        "USD": 11.044
    },
    "timestamp": "2025-07-28T19:45:14.904727+00:00"
}
```


Internal API URL: `INTERNAL_API_SERVER_URL + "cryptoprices/fixed_quote/"`

```python
@app.get("/cryptoprices/fixed_quote/", tags=["crypto"])
async def fixed_hive(
    HIVE: float = None, HBD: float = None, USD: float = None, cache_time: int = 600
) -> FixedHiveQuote:
    """Returns a FixedHiveQuote which will be cached in Redis for cache_time seconds"""
    cp = CryptoPrices()
    if not (HIVE or HBD or USD):
        raise HTTPException(
            status_code=422, detail="You must specify one of HBD, HIVE or USD"
        )
    conv = CryptoConversion(HIVE=HIVE, HBD=HBD, USD=USD)
    if conv.conv_from == "USD":
        await cp.get_all_prices()
        conv.conv_from = "HIVE"
        conv.HIVE = (USD * (1.0 + Config.MARGIN_SPREAD)) / cp.Hive_USD
    fixed_hive_quote = await cp.get_fixed_hive_sats(
        amount_req=conv.beem_amount, cache_time=cache_time
    )
    return fixed_hive_quote




class CryptoConversion(BaseModel):
    """Holds the result of a conversion of a crypto amount"""

    conv_from: str = None
    sats: int = None
    HIVE: float = None
    HBD: float = None
    USD: float = None

    def __init__(__pydantic_self__, amount: Amount = None, **data: Any):
        if amount:
            conv_from = amount.symbol
            data = {"conv_from": amount.symbol, amount.symbol: amount.amount}
            super().__init__(**data)
            return

        if len(data) == 0 or len(data) == 5:
            super().__init__(**data)
            return

        conv_from = data.get("conv_from")
        if conv_from is None:
            if len(data) == 1:
                for key, _ in data.items():
                    conv_from = key
            elif len(data) > 1:
                conv_from = [key for key, value in data.items() if value is not None][0]
        super().__init__(conv_from=conv_from, **data)

    @property
    def amount(self) -> Union[float, int, None]:
        """Returns the original amount for the conv_from currency."""
        if self.conv_from:
            return self.__getattribute__(self.conv_from)
        else:
            return None

    @property
    def beem_amount(self) -> Amount:
        """Returns the BEEM amount for this conversion in Hive or HBD"""
        if self.amount:
            if self.conv_from == "HBD":
                ans = Amount(amount=self.HBD, asset="HBD")
            else:
                ans = Amount(amount=self.HIVE, asset="HIVE")
            return ans
        return None

    def invert(self):
        """Invert the sign of all values."""
        self.sats = -self.sats
        self.HIVE = -self.HIVE
        self.HBD = -self.HBD
        self.USD = -self.USD
        return self


class FixedHiveQuote(BaseModel):
    """Holds a price quote in sats for fixed amount of Hive"""

    unique_id: UUID
    sats_send: int
    conv: CryptoConversion
    timestamp: datetime = datetime.now(tz=timezone.utc)

    @property
    def beem_amount(self) -> Amount:
        """Return the amount to receive as a Beem amount"""
        return self.conv.beem_amount
```
