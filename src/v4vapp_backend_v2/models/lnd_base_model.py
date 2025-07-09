from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv


class LNDBaseModel(TrackedBaseModel):
    conv: CryptoConv = CryptoConv()
    """
    Base model for LND-related data.
    """

    def __init__(self, **data):
        """
        Initialize the LNDBaseModel with the provided data.

        :param data: The data to initialize the model with.
        """
        super().__init__(**data)
        self.conv = data.get("conv", CryptoConv())