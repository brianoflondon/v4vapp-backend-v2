async def get_walletofsatoshi_invoice(
    amount_sats: int = 1000, memo: str = "Test Invoice for v4vapp"
) -> str:
    """
    Helper function to retrieve a Lightning invoice from Wallet of Satoshi for testing purposes.

    This function performs the following steps:
    1. Sends a request to the Wallet of Satoshi API to create a new Lightning invoice with a specified amount and memo.
    2. Parses the response to extract the payment request (invoice) and returns it.

    Returns:
        str: The payment request (Lightning invoice) retrieved from Wallet of Satoshi.

    Raises:
        Exception: If there is an error in retrieving the invoice from Wallet of Satoshi.
    """
    import httpx

    lnurlp_url = "https://walletofsatoshi.com/.well-known/lnurlp/brianoflondon"
    try:
        # Step 1: fetch the LNURL pay metadata to get the callback URL
        lnurlp_response = httpx.get(lnurlp_url)
        lnurlp_response.raise_for_status()
        lnurlp_data = lnurlp_response.json()
        callback = lnurlp_data["callback"]

        # Step 2: call the callback with amount in msats
        amount_msats = amount_sats * 1000
        params: dict = {"amount": amount_msats}
        if memo:
            params["comment"] = memo
        invoice_response = httpx.get(callback, params=params)
        invoice_response.raise_for_status()
        invoice_data = invoice_response.json()
        return invoice_data.get("pr")
    except Exception as e:
        print(f"Failed to retrieve invoice from Wallet of Satoshi: {e}")
        raise
