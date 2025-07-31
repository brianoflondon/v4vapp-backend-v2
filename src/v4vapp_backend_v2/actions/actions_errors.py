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
