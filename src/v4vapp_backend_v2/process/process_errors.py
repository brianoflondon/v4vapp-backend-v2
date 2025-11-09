class HiveLightningError(Exception):
    """
    Custom exception for errors related to Hive and Lightning operations.
    This can be used to handle specific cases where the interaction between
    Hive and Lightning fails or is not implemented.
    """

    pass


class HiveToLightningError(HiveLightningError):
    """
    Custom exception for Hive to Lightning errors.
    """

    pass


class LightningToHiveError(HiveLightningError):
    """
    Custom exception for Lightning to Hive errors.
    This can be used to handle specific cases where the conversion or transfer fails.
    """

    pass


class KeepsatsDepositNotificationError(HiveLightningError):
    """
    Custom exception for errors related to Keepsats deposit notifications.
    This can be used to handle specific cases where the notification process fails.
    """

    pass


class CustomJsonToLightningError(HiveLightningError):
    """
    Custom exception for errors related to processing CustomJson data to Lightning.
    This can be used to handle specific cases where the conversion or processing fails.
    """

    pass


class CustomJsonAuthorizationError(HiveLightningError):
    """
    Custom exception for errors related to CustomJson authorization.
    This can be used to handle specific cases where the authorization fails.
    """

    pass


class CustomJsonRetryError(HiveLightningError):
    """
    Custom exception for errors related to retrying CustomJson processing.
    This can be used to handle specific cases where retries are exhausted or fail.
    """

    def __init__(self, message: str, attempts: int = 0):
        super().__init__(message)
        self.attempts = attempts


class InsufficientBalanceError(HiveLightningError):
    """
    Custom exception for errors related to insufficient balance.
    This can be used to handle specific cases where an account does not have enough funds for a transfer.
    """

    pass
