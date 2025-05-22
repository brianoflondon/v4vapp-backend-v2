from typing import Union

from v4vapp_backend_v2.hive_models.op_all import OpAny
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment

TrackedAny = Union[OpAny, Invoice, Payment]
#TODO: #111 implement discriminator in models to pick the right one

# TrackedAny = Annotated[(OpAny, Invoice, Payment), Tag("tracked_any")]