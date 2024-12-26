import json
import posixpath
import tempfile

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.events.async_event import async_subscribe
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.models.lnd_models import LNDInvoice


class MyDBFlat:
    most_recent: LNDInvoice
    most_recent_settled: LNDInvoice

    def __init__(self):
        self.most_recent = LNDInvoice.model_construct()
        self.most_recent_settled = LNDInvoice.model_construct()
        async_subscribe(Events.LND_INVOICE, self.update_most_recent)

    async def update_most_recent(self, invoice: LNDInvoice):
        if invoice.settled:
            self.most_recent_settled = invoice
        else:
            self.most_recent = invoice







class MyDB:
    class LND:
        most_recent: LNDInvoice
        most_recent_settled: LNDInvoice

    def __init__(self):
        self._TEMP_FILE = posixpath.join(tempfile.gettempdir(), "database.json")
        self.LND.most_recent = LNDInvoice.model_construct()
        self.LND.most_recent_settled = LNDInvoice.model_construct()

        try:
            with open(self._TEMP_FILE, "r") as f:
                invoices_json = json.load(f)

                self.LND.most_recent = LNDInvoice.model_construct(
                    invoices_json["most_recent"]
                )
                self.LND.most_recent_settled = LNDInvoice.model_construct(
                    invoices_json["most_recent_settled"]
                )
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"File {self._TEMP_FILE} not found.")
            logger.warning(e, extra={"json": {"file": self._TEMP_FILE}})
            logger.warning(f"Most recent invoice: {self.LND.most_recent}")

    def update_most_recent(self, invoice: LNDInvoice):
        output = {}
        if invoice.settled:
            self.LND.most_recent_settled = invoice
            output = {
                "most_recent": self.LND.most_recent.model_dump(),
                "most_recent_settled": self.LND.most_recent_settled.model_dump(),
            }
        else:
            self.LND.most_recent = invoice
            output = {
                "most_recent": self.LND.most_recent.model_dump(),
                "most_recent_settled": self.LND.most_recent_settled.model_dump(),
            }

        with open(self._TEMP_FILE, "w") as f:
            json.dump(output, f, default=str)
            logger.debug(
                f"Updated most recent invoice: {invoice.add_index} {invoice.settled}",
                extra=output,
            )


# Create a temporary file
db = MyDBFlat()

# subscribe(Events.LND_INVOICE_CREATED, db.update_most_recent)
