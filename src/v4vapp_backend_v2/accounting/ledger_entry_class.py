import textwrap
from datetime import datetime, timezone
from decimal import Decimal
from math import isclose
from typing import Any, Dict, List, Self

from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    computed_field,
    field_validator,
    model_validator,
)
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError
from pymongo.results import InsertOneResult, UpdateResult

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    NORMAL_CREDIT_ACCOUNTS,
    NORMAL_DEBIT_ACCOUNTS,
    AssetAccount,
    LedgerAccountAny,
)
from v4vapp_backend_v2.accounting.ledger_cache import invalidate_ledger_cache
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType, LedgerTypeIcon
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_tools import convert_decimal128_to_decimal
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    convert_decimals_for_mongodb,
    snake_case,
)
from v4vapp_backend_v2.helpers.lightning_memo_class import LightningMemo
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType


class LedgerEntryException(Exception):
    """Custom exception for LedgerEntry errors."""

    pass


class LedgerEntryCreationException(LedgerEntryException):
    """Custom exception for LedgerEntry creation errors."""

    pass


class LedgerEntryConfigurationException(LedgerEntryException):
    """Custom exception for LedgerEntry configuration errors."""

    pass


class LedgerEntryDuplicateException(LedgerEntryException):
    """Custom exception for LedgerEntry duplicate errors."""

    pass


class LedgerEntryNotFoundException(LedgerEntryException):
    """Custom exception for LedgerEntry not found errors."""

    pass


class LedgerEntry(BaseModel):
    """
        short_id (str): Short ID for the ledger entry.
        user_memo (str): A memo which can be shown to users for the ledger entry.
        debit (LedgerAccountAny): Account to be debited.
        credit (LedgerAccountAny): Account to be credited.

        __init__(self, **data): Initializes a LedgerEntry instance.
        debit_amount_signed(self) -> Decimal: Returns the debit amount as a signed value.
        credit_amount_signed(self) -> Decimal: Returns the credit amount as a signed value.
        debit_sign(self) -> int: Returns the sign of the debit amount.
        credit_sign(self) -> int: Returns the sign of the credit amount.
        conv_signed(self) -> Dict[str, CryptoConv]: Returns conversion details as signed values.
        log_str(self) -> str: Returns a string representation for logging purposes.
        collection_name(cls) -> str: Returns the name of the associated database collection.
        collection(cls) -> AsyncCollection: Returns the database collection object.
        save(self) -> InsertOneResult: Asynchronously saves the LedgerEntry to the database.

    Exceptions:
        LedgerEntryCreationException: Raised if the entry is not completed or errors occur during DB operations.
        LedgerEntryConfigurationException: Raised if the database client is not configured.
        LedgerEntryDuplicateException: Raised if a duplicate ledger entry is detected.

    LedgerEntry represents a single accounting transaction in the ledger system, encapsulating both debit and credit sides, conversion details, and metadata for database operations.

    Attributes:
        group_id (str): Group ID for the ledger entry.
        ledger_type (LedgerType): Transaction type of the ledger entry.
        timestamp (datetime): Timestamp of the ledger entry.
        description (str): Description of the ledger entry.
        cust_id (AccNameType): Customer ID associated with the ledger entry.
        debit_amount (float): Amount of the debit transaction.
        debit_unit (Currency): Unit of the debit transaction.
        debit_conv (CryptoConv): Conversion details for the debit transaction.
        credit_amount (float): Amount of the credit transaction.
        credit_unit (Currency): Unit of the credit transaction.
        credit_conv (CryptoConv): Conversion details for the credit transaction.
        debit (LedgerAccountAny | None): Account to be debited.
        credit (LedgerAccountAny | None): Account to be credited.
        op (TrackedAny | None): Associated operation.
        op_type (str): Type of the operation, defaults to 'ledger_entry'.
        extra_data (List[Any]): Additional data related to the ledger entry.
        model_config (ConfigDict): Model configuration.

    Methods:
        __init__(self, **data): Initializes a LedgerEntry instance, sets op_type.
        credit_debit_equality(self) -> Self: Validates equality of debit and credit amounts and conversions.
        ledger_type_str(self) -> str: Returns a human-readable string representation of the ledger type.
        is_completed(self) -> bool: Checks if the ledger entry is completed and balanced.
        credit_debit_balance_str(self) -> str: Returns a message if debit and credit conversions mismatch.
        credit_debit(self) -> tuple[LedgerAccountAny | None, LedgerAccountAny | None]: Returns credit and debit accounts.
        __repr__(self) -> str: Returns a data representation of the LedgerEntry.
        __str__(self) -> str: Returns a formatted journal entry string.
        name(cls) -> str: Returns the class name in snake_case format.
        log_extra(self) -> Dict[str, Any]: Generates additional logging information.
        group_id_query(self) -> dict[str, Any]: Returns a MongoDB query for this record.
        short_id(self) -> str: Returns a short identifier for the LedgerEntry.
        collection(cls) -> str: Returns the name of the associated database collection.
        db_checks(self) -> None: Performs checks to ensure the entry is valid for saving.
        update_op(self) -> UpdateResult: Asynchronously updates the ledger entry in the database.
        save(self) -> ObjectId: Saves the LedgerEntry to the database (should only be called once).
        draw_t_diagram(self) -> str: Draws a T-diagram for the LedgerEntry, showing both sides and conversion details.
        print_journal_entry(self) -> str: Prints a formatted journal entry, showing currencies and conversion.

        LedgerEntryCreationException: If the entry is not completed or errors occur during DB operations.

    Usage:
        - Create a LedgerEntry to represent a transaction.
        - Validate and save to the database.
        - Use provided methods for logging, display, and database operations.
    """

    group_id: str = Field("", description="Group ID for the ledger entry")
    short_id: str = Field("", description="Short ID for the ledger entry")
    ledger_type: LedgerType = Field(
        default=LedgerType.UNSET, description="Transaction type of the ledger entry"
    )
    timestamp: datetime = Field(
        datetime.now(tz=timezone.utc), description="Timestamp of the ledger entry"
    )
    description: str = Field("", description="Description of the ledger entry")
    user_memo: str = Field(
        "", description="A memo which can be shown to users for the ledger entry"
    )
    cust_id: AccNameType = Field(
        "", description="Customer ID of any type associated with the ledger entry"
    )
    debit_amount: Decimal = Field(Decimal(0), description="Amount of the debit transaction")
    debit_unit: Currency = Field(
        default=Currency.HIVE, description="Unit of the debit transaction"
    )
    debit_conv: CryptoConv = Field(
        default_factory=CryptoConv, description="Conversion details for the debit transaction"
    )
    credit_amount: Decimal = Field(Decimal(0), description="Amount of the credit transaction")
    credit_unit: Currency = Field(
        default=Currency.HIVE, description="Unit of the credit transaction"
    )
    credit_conv: CryptoConv = Field(
        default_factory=CryptoConv, description="Conversion details for the credit transaction"
    )
    debit: LedgerAccountAny = Field(
        AssetAccount(
            name="Unset",
        ),
        description="Account to be debited",
    )
    credit: LedgerAccountAny = Field(
        AssetAccount(
            name="Unset",
        ),
        description="Account to be credited",
    )
    op_type: str = Field(
        default="ledger_entry",
        description="Type of the operation, defaults to 'ledger_entry'",
    )
    link: str = Field("", description="Link to the Hive block explorer transaction if appropriate")
    extra_data: List[Any] = Field(
        default_factory=list, description="Additional data related to the ledger entry"
    )

    @field_validator("debit_conv", "credit_conv", "credit_amount", "debit_amount", mode="before")
    @classmethod
    def convert_mongodb_decimals(cls, v):
        """Convert Decimal128 objects from MongoDB to Decimal objects for Pydantic validation."""
        return convert_decimal128_to_decimal(v)

    def __init__(self, **data):
        super().__init__(**data)

    @model_validator(mode="after")
    def credit_debit_equality(self) -> Self:
        if self.credit_unit == self.debit_unit and self.credit_amount == self.debit_amount:
            return self
        if self.credit_conv == self.debit_conv:
            credit_amount_in_debit_unit = getattr(self.credit_conv, self.debit_unit)
            debit_amount_in_credit_unit = getattr(self.debit_conv, self.credit_unit)
            if isclose(
                self.debit_amount,
                credit_amount_in_debit_unit,
                rel_tol=CryptoConv.REL_TOL,
                abs_tol=CryptoConv.UNIT_TOLERANCE[self.debit_unit.value],
            ):
                return self
            if isclose(
                self.credit_amount,
                debit_amount_in_credit_unit,
                rel_tol=CryptoConv.REL_TOL,
                abs_tol=CryptoConv.UNIT_TOLERANCE[self.credit_unit.value],
            ):
                return self
            logger.warning(
                f"Debit and Credit amounts do not match "
                f"{credit_amount_in_debit_unit} != {self.debit_amount} "
                f"and {debit_amount_in_credit_unit} != {self.credit_amount}",
                extra={"notification": False, **self.log_extra},
            )
        logger.warning(
            "Debit and credit conv values out of tolerance",
            extra={"notification": False, **self.log_extra},
        )
        return self

    @computed_field
    def debit_amount_signed(self) -> Decimal:
        """
        Returns the debit amount as a signed value.
        This is used to ensure that the debit amount is always positive in accounting terms.
        """
        return self.debit_amount * self.debit_sign

    @computed_field
    def credit_amount_signed(self) -> Decimal:
        """
        Returns the credit amount as a signed value.
        This is used to ensure that the credit amount is always positive in accounting terms.
        """
        return self.credit_amount * self.credit_sign

    @property
    def icon(self) -> str:
        icon = LedgerTypeIcon.get(self.ledger_type, "❓")
        return icon

    @property
    def debit_sign(self) -> int:
        """
        Returns the sign of the debit amount.
        This is used to determine if the debit amount is positive or negative.

        """
        if self.debit.account_type in NORMAL_DEBIT_ACCOUNTS:
            return 1
        else:
            return -1

    @property
    def credit_sign(self) -> int:
        """
        Returns the sign of the credit amount.
        This is used to determine if the credit amount is positive or negative.
        """
        if self.credit.account_type in NORMAL_CREDIT_ACCOUNTS:
            return 1
        else:
            return -1

    @computed_field
    def conv_signed(self) -> Dict[str, CryptoConv]:
        """
        Returns the conversion details as a signed CryptoConv object.
        This is used to ensure that the conversion amounts are correctly signed for accounting.
        """
        return {
            "debit": self.debit_conv * self.debit_sign,
            "credit": self.credit_conv * self.credit_sign,
        }

    @property
    def ledger_type_str(self) -> str:
        """Returns the string representation of the ledger type.

        This property is used to provide a human-readable format of the ledger type,
        which can be useful for logging or displaying in user interfaces.

        Returns:
            str: The string representation of the ledger type.
        """
        return self.ledger_type.printout
        # ans = " ".join(word.capitalize() for word in self.ledger_type.name.split("_"))
        # ans = f"{ans} ({self.ledger_type.name} {self.ledger_type.value})"
        # return ans

    @property
    def is_completed(self) -> bool:
        if not self.debit and not self.credit:
            return False
        if self.debit_amount is None or self.credit_amount is None:
            return False
        if message := self.credit_debit_balance_str:
            logger.error(message, extra={"notification": False, **self.log_extra})
            return False
        return True

    @property
    def credit_debit_balance_str(self) -> str:
        """
        Returns a message indicating a mismatch between debit and credit conversion amounts if
        their values differ by more than a specified tolerance.

        Returns:
            str: An error message detailing the mismatch between debit and credit conversions,
            including group ID and ledger type, if the amounts differ by more than 10 msats.
            Returns an empty string if the amounts are within the tolerance.
        """
        if not isclose(self.debit_conv.msats, self.credit_conv.msats, rel_tol=0.1, abs_tol=10):
            message = (
                f"Debit and Credit Conversion mismatch {self.group_id} {self.ledger_type_str}: "
                f"{self.debit_conv.msats} vs {self.credit_conv.msats}"
            )
            return message
        return ""

    @property
    def credit_debit(self) -> tuple[LedgerAccountAny | None, LedgerAccountAny | None]:
        """
        Returns a tuple of the credit and debit accounts.
        """
        return self.credit, self.debit

    def __repr__(self) -> str:
        """
        Returns a data representation of the LedgerEntry.
        """
        return (
            f"LedgerEntry(group_id={self.group_id}, timestamp={self.timestamp}, description={self.description}, "
            f"debit_amount={self.debit_amount}, debit_unit={self.debit_unit}, "
            f"credit_amount={self.credit_amount}, credit_unit={self.credit_unit}, "
            f"debit={self.debit}, credit={self.credit})"
        )

    def __str__(self) -> str:
        """
        Returns a string representation of the LedgerEntry.
        """
        return self.print_journal_entry()

    @classmethod
    def name(cls) -> str:
        """
        Returns the name of the class in snake_case format.

        This method converts the class name to a snake_case string
        representation, which is typically used for naming operations
        or identifiers in a consistent and readable format.

        Returns:
            str: The snake_case representation of the class name.
        """
        return snake_case(cls.__name__)

    @property
    def log_extra(self) -> Dict[str, Any]:
        """
        Generates a dictionary containing additional logging information.
        Usage: in a log entry use as an unpacked dictionary like this:
        `logger.info(f"{op.block_num} | {op.log_str}", extra={**op.log_extra})`

        Returns:
            Dict[str, Any]: A dictionary where the key is the name of the current instance
            and the value is the serialized representation of the instance, excluding the
            "raw_op" field.
        """
        return {self.name(): self.model_dump(by_alias=True, exclude_none=True, exclude_unset=True)}

    @property
    def log_str(self) -> str:
        """
        Returns a string representation of the LedgerEntry for logging purposes.

        This method is used to provide a concise and informative string that can be used in logs,
        which includes the group_id, ledger_type, timestamp, and description.

        Returns:
            str: A formatted string representation of the LedgerEntry.
        """
        formatted_time = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        formatted_amount = (
            f"{self.credit_amount:.3f} {self.credit_unit}"
            if self.credit_unit != Currency.MSATS
            else f"{self.credit_amount // 1000:.0f} sats"
        )
        return (
            f"{formatted_time} | "
            f"{self.ledger_type_str:<35} | {formatted_amount:>20} | "
            f"{self.credit} | {self.debit} | "
            f"{self.description}"
        )

    @property
    def group_id_query(self) -> dict[str, Any]:
        """
        Returns a Mongodb Query for this record.

        This method is used to determine the key in the database where
        the operation data will be stored. It is typically used for
        database operations and indexing.

        The mongodb is a compound of these three fields (and also the realm)

        Returns:
            dict: A dictionary containing the group_id for the ledger entry.
        """
        ans = {
            "group_id": self.group_id,
        }
        return ans

    # MARK: DB Database Methods

    @classmethod
    def collection_name(cls) -> str:
        """
        Returns the name of the collection associated with this model.

        This method is used to determine where the operation data will be stored
        in the database.

        Returns:
            str: The name of the collection.
        """
        return "ledger"

    @classmethod
    def collection(cls) -> AsyncCollection:
        """
        Returns the collection associated with this model.
        Remember to use this with the parenthesis:
        ```
        existing_entry_raw = await LedgerEntry.collection().find_one(
            filter={"group_id": group_id},
        )
        ```

        This method is used to get the collection from the database client,
        which is necessary for performing database operations.

        Returns:
            AsyncCollection: The collection associated with this model.
        """
        return InternalConfig.db["ledger"]

    @classmethod
    async def load(cls, group_id: str) -> "LedgerEntry | None":
        """
        Load a single LedgerEntry by group_id or return None if not found.
        """
        doc = await cls.collection().find_one(filter={"group_id": group_id})
        try:
            return cls.model_validate(doc) if doc else None
        except ValidationError as e:
            logger.error(f"Error validating ledger entry: {e}")
            return None

    # ...existing code...

    def db_checks(self) -> None:
        """
        Performs database checks to ensure the LedgerEntry is valid for saving.
        This method checks if the LedgerEntry is completed and if the database client is configured.
        It raises exceptions if the checks fail.

        Raises:
            LedgerEntryCreationException: If the ledger entry is not completed or if an error occurs during checks.
            LedgerEntryConfigurationException: If the database client is not configured.
        """
        if not self.is_completed:
            raise LedgerEntryCreationException("LedgerEntry is not completed.")
        # Additional integrity checks for specific ledger types
        try:
            from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
        except Exception:
            LedgerType = None

        # Helper to convert values to float safely
        def _to_num(v):
            try:
                return float(v)
            except Exception:
                return 0.0

        # If this is an exchange conversion, ensure the conversion sides net to zero
        if LedgerType is not None and self.ledger_type == LedgerType.EXCHANGE_CONVERSION:
            conv = self.conv_signed
            debit_conv = conv.get("debit", None)  # type: ignore
            credit_conv = conv.get("credit", None)  # type: ignore
            if not debit_conv or not credit_conv:
                raise LedgerEntryCreationException(
                    "Missing conversion details for exc_conv entry."
                )

            # msats should sum to zero (canonical integer unit)
            msats_sum = _to_num(getattr(debit_conv, "msats", 0)) + _to_num(
                getattr(credit_conv, "msats", 0)
            )
            if abs(msats_sum) > 10:  # tolerate up to 10 msats rounding noise
                raise LedgerEntryCreationException(
                    f"exc_conv msats do not net to zero: {msats_sum} msats for {self.group_id}"
                )

            # hive should sum to zero within unit tolerance
            hive_sum = _to_num(getattr(debit_conv, "hive", 0)) + _to_num(
                getattr(credit_conv, "hive", 0)
            )
            if abs(hive_sum) > CryptoConv.UNIT_TOLERANCE[Currency.HIVE.value]:
                raise LedgerEntryCreationException(
                    f"exc_conv hive values do not net to zero: {hive_sum} HIVE for {self.group_id}"
                )

            # Warn if conversion fetch_dates are missing since conversions rely on external quotes
            if not getattr(debit_conv, "fetch_date", None) or not getattr(
                credit_conv, "fetch_date", None
            ):
                logger.warning(
                    "Missing conversion fetch_date on exc_conv entry; fetched prices may be incomplete",
                    extra={"notification": False, **self.log_extra},
                )

        # If this is an exchange fee, ensure msats/netting holds
        if LedgerType is not None and self.ledger_type == LedgerType.EXCHANGE_FEES:
            conv = self.conv_signed
            debit_conv = conv.get("debit", None)  # pyright: ignore[reportAttributeAccessIssue]
            credit_conv = conv.get("credit", None)  # pyright: ignore[reportAttributeAccessIssue]
            if debit_conv and credit_conv:
                msats_sum = _to_num(getattr(debit_conv, "msats", 0)) + _to_num(
                    getattr(credit_conv, "msats", 0)
                )
                # Fees should net to zero across debit/credit sides (one side is expense, other asset)
                if abs(msats_sum) > 10:
                    raise LedgerEntryCreationException(
                        f"exc_fee msats do not net to zero: {msats_sum} msats for {self.group_id}"
                    )

    async def save(
        self, ignore_duplicates: bool = False, upsert: bool = False
    ) -> InsertOneResult | UpdateResult | None:
        """
        Saves the LedgerEntry to the database. This should only be called after the LedgerEntry is completed.
        and once. If it is called again, it will raise a duplicate exception.

        Raises:
            LedgerEntryCreationException: If the ledger entry is not completed or if an error occurs during saving.
            LedgerEntryConfigurationException: If the database client is not configured.
            LedgerEntryDuplicateException: If a duplicate ledger entry is detected.
        Returns:
            InsertOneResult: The result of the insert operation.
        Side effects:
            - Inserts the LedgerEntry into the database.
            - Logs the operation details.

        """
        self.db_checks()
        try:
            # Get the model dump and convert Decimal objects to strings for MongoDB compatibility
            document: Any = self.model_dump(by_alias=True, exclude_none=True, exclude_unset=True)
            document = convert_decimals_for_mongodb(document)

            ans: InsertOneResult | UpdateResult | None = None
            if not upsert:
                ans = await InternalConfig.db["ledger"].insert_one(document=document)
            else:
                ans = await InternalConfig.db["ledger"].update_one(
                    filter=self.group_id_query,
                    update={"$set": document},
                    upsert=True,
                )
                logger.info(
                    f"Ledger Entry upserted: {self.group_id}",
                    extra={"notification": False, "db_ans": ans},
                )
            logger.debug(f"Ledger Entry saved: {self.group_id}")
            logger.debug(
                f"\n{self}",
                extra={"notification": False, "db_ans": ans, **self.log_extra},
            )

            # Invalidate all cached ledger balances so subsequent reads reflect this entry
            await invalidate_ledger_cache(
                debit_name=self.debit.name,
                debit_sub=self.debit.sub,
                credit_name=self.credit.name,
                credit_sub=self.credit.sub,
            )

            return ans
        except DuplicateKeyError as e:
            if not ignore_duplicates:
                logger.warning(
                    f"Duplicate ledger entry detected: {e}",
                    extra={"notification": False, **self.log_extra},
                )
                raise LedgerEntryDuplicateException(f"Duplicate ledger entry detected: {e}")
            else:
                logger.debug("Duplicate ledger entry ignored.")
                return None

        except Exception as e:
            logger.error(
                f"Error saving ledger entry to database: {e}",
                extra={"notification": True, **self.log_extra},
            )
            raise LedgerEntryCreationException(f"Error saving ledger entry: {e}") from e

    def draw_t_diagram(self) -> str:
        """
        Draws a T-diagram for the LedgerEntry, showing account names, sub-values, account types, memo,
        and conversion values for both debit and credit sides.
        """
        # Extract fields
        debit_name = self.debit.name if self.debit else ""
        debit_sub = self.debit.sub if self.debit and self.debit.sub else ""
        debit_type = (
            self.debit.account_type if self.debit and self.debit.account_type else "Unknown"
        )
        credit_name = self.credit.name if self.credit else ""
        credit_sub = self.credit.sub if self.credit and self.credit.sub else ""
        credit_type = (
            self.credit.account_type if self.credit and self.credit.account_type else "Unknown"
        )
        description = self.description or ""
        debit_amount = self.debit_amount
        debit_unit = self.debit_unit.value if self.debit_unit else ""
        credit_amount = self.credit_amount
        credit_unit = self.credit_unit.value if self.credit_unit else ""
        debit_conv = self.debit_conv
        credit_conv = self.credit_conv

        # Truncate description if too long
        max_desc_len = 50
        if len(description) > max_desc_len:
            description = description[: max_desc_len - 3] + "..."

        # Define column widths
        account_width = max(len(debit_name), len(credit_name), 35)
        sub_width = max(len(debit_sub), len(credit_sub), 20)
        type_width = max(len(debit_type), len(credit_type), 10)
        side_width = account_width + type_width + sub_width + 4
        total_width = side_width * 2 + 4 + 3

        # Build T-diagram
        lines = []

        # Header
        lines.append("=" * total_width)
        lines.append(f"{self.group_id:^{total_width}}")
        lines.append("=" * total_width)

        # Account headers
        lines.append(f"| {'Debit':<{side_width}} | {'Credit':<{side_width}} |")
        lines.append(f"| {'-' * side_width} | {'-' * side_width} |")

        # Account names, types, and sub-values
        debit_display = f"{debit_name} ({debit_type})"
        credit_display = f"{credit_name} ({credit_type})"
        lines.append(
            f"| {debit_display:<{account_width + type_width + 2}} {debit_sub:<{sub_width + 1}} | "
            f"{credit_display:<{account_width + type_width + 2}} {credit_sub:<{sub_width + 1}} |"
        )

        # Amounts and units
        debit_amount_str = f"{debit_amount:.3f} {debit_unit}"
        credit_amount_str = f"{credit_amount:.3f} {credit_unit}"
        lines.append(f"| {debit_amount_str:<{side_width}} | {credit_amount_str:<{side_width}} |")

        # Footer
        lines.append("=" * total_width)

        # Description
        lines.append(f"Description: {description}")
        lines.append("-" * total_width)

        # Conversion values for debit
        if debit_conv:
            lines.append("Debit Conversion Values (at time of entry):")
            lines.append(f"{'Currency':<10} | {'Value':>10} | {'Rate':>15}")
            lines.append(f"{'-' * 10}-+-{'-' * 10}-+-{'-' * 15}")
            lines.append(
                f"{'HIVE':<10} | {debit_conv.hive:>10.3f} | {debit_conv.sats_hive:>15.2f} Sats/HIVE"
            )
            lines.append(
                f"{'HBD':<10} | {debit_conv.hbd:>10.3f} | {debit_conv.sats_hbd:>15.2f} Sats/HBD"
            )
            lines.append(f"{'USD':<10} | {debit_conv.usd:>10.3f} |")
            lines.append(f"{'SATS':<10} | {debit_conv.sats:>10} |")
            lines.append(f"{'BTC':<10} | {debit_conv.btc:>10.8f} |")
            if debit_conv.fetch_date:
                lines.append(f"Fetched: {debit_conv.fetch_date.strftime('%Y-%m-%d %H:%M:%S')}")
            lines.append(f"Source: {debit_conv.source}")
            lines.append("-" * total_width)

        # Conversion values for credit
        if credit_conv:
            lines.append("Credit Conversion Values (at time of entry):")
            lines.append(f"{'Currency':<10} | {'Value':>10} | {'Rate':>15}")
            lines.append(f"{'-' * 10}-+-{'-' * 10}-+-{'-' * 15}")
            lines.append(
                f"{'HIVE':<10} | {credit_conv.hive:>10.3f} | {credit_conv.sats_hive:>15.2f} Sats/HIVE"
            )
            lines.append(
                f"{'HBD':<10} | {credit_conv.hbd:>10.3f} | {credit_conv.sats_hbd:>15.2f} Sats/HBD"
            )
            lines.append(f"{'USD':<10} | {credit_conv.usd:>10.3f} |")
            lines.append(f"{'SATS':<10} | {credit_conv.sats:>10} |")
            lines.append(f"{'BTC':<10} | {credit_conv.btc:>10.8f} |")
            if credit_conv.fetch_date:
                lines.append(f"Fetched: {credit_conv.fetch_date.strftime('%Y-%m-%d %H:%M:%S')}")
            lines.append(f"Source: {credit_conv.source}")
        lines.append("=" * total_width)

        return "\n".join(lines)

    def print_journal_entry(self) -> str:
        """
        Prints a formatted journal entry, showing different currencies for debit and credit if applicable.

        Returns:
            str: A string representation of the journal entry.
        """
        max_width = 100
        if not self.is_completed or not self.debit or not self.credit:
            # If the entry is not completed, show a warning
            return (
                f"WARNING: LedgerEntry is not completed. Missing debit or credit account.\n"
                f"{'=' * max_width}\n"
            )

        # Prepare the account names with type in parentheses
        debit_account = self.debit.name if self.debit else "N/A"
        debit_type = (
            self.debit.account_type
            if self.debit and hasattr(self.debit, "account_type")
            else "N/A"
        )
        credit_account = self.credit.name if self.credit else "N/A"
        credit_type = (
            self.credit.account_type
            if self.credit and hasattr(self.credit, "account_type")
            else "N/A"
        )
        debit_account_with_type = f"{debit_account} ({debit_type})"
        credit_account_with_type = f"{credit_account} ({credit_type})"

        # Determine display units and conversion for debit and credit
        debit_display_unit = (
            "SATS"
            if self.debit_unit and self.debit_unit.value.upper() == "MSATS"
            else self.debit_unit.value
            if self.debit_unit
            else ""
        )
        credit_display_unit = (
            "SATS"
            if self.credit_unit and self.credit_unit.value.upper() == "MSATS"
            else self.credit_unit.value
            if self.credit_unit
            else ""
        )
        debit_conversion_factor = (
            1000 if self.debit_unit and self.debit_unit.value.upper() == "MSATS" else 1
        )
        credit_conversion_factor = (
            1000 if self.credit_unit and self.credit_unit.value.upper() == "MSATS" else 1
        )

        debit_contra_str = "-c-" if self.debit.contra else "   "
        credit_contra_str = "-c-" if self.credit.contra else "   "

        # Format the amounts: SATS with no decimals and commas, others with 2 decimals
        debit_amount = self.debit_amount if self.debit_amount else 0.00
        credit_amount = self.credit_amount if self.credit_amount else 0.00

        if debit_display_unit.upper() == "SATS" and (debit_amount / debit_conversion_factor) < 5:
            formatted_debit_amount = (
                f"{debit_amount / debit_conversion_factor:,.3f} {debit_display_unit.upper()}"
            )
        elif debit_conversion_factor == 1000:
            formatted_debit_amount = (
                f"{debit_amount / debit_conversion_factor:,.0f} {debit_display_unit.upper()}"
            )
        else:
            formatted_debit_amount = f"{debit_amount:,.3f} {debit_display_unit.upper()}"

        if (
            credit_display_unit.upper() == "SATS"
            and (credit_amount / credit_conversion_factor) < 5
        ):
            formatted_credit_amount = (
                f"{credit_amount / credit_conversion_factor:,.3f} {credit_display_unit.upper()}"
            )
        elif credit_conversion_factor == 1000:
            formatted_credit_amount = (
                f"{credit_amount / credit_conversion_factor:,.0f} {credit_display_unit.upper()}"
            )
        else:
            formatted_credit_amount = f"{credit_amount:,.3f} {credit_display_unit.upper()}"

        formatted_credit_amount = f"{credit_contra_str} {formatted_credit_amount}"
        formatted_debit_amount = f"{debit_contra_str} {formatted_debit_amount}"

        description = LightningMemo(self.description).short_memo
        if len(description) > 100:
            # Split description into lines at word boundaries, max 100 chars per line
            description = "\n".join(textwrap.wrap(description, width=100))
        if self.credit_debit_balance_str:
            description += f"{description}\n{self.credit_debit_balance_str}"

        # Create a conversion line which looks
        # like Converted              -0.000 HIVE       -0.000 HBD       -0.000 USD           -0 SATS               -0 msats
        if self.debit_conv and self.credit_conv:
            conversion_line = (
                f"Converted   "
                f"{self.debit_conv.hive:>11,.3f} HIVE "
                f"{self.debit_conv.hbd:>11,.3f} HBD "
                f"{self.debit_conv.usd:>11,.3f} USD "
                f"{self.debit_conv.sats_rounded:>18,.3f} SATS "
            )
        else:
            conversion_line = "Converted              N/A"

        entry = f"J/E NUMBER  : {self.group_id or '#####'}\nLEDGER TYPE : {self.icon}{self.ledger_type_str:<40}\n"
        # Build combined CUSTOMER_ID + right-aligned date line
        line_width = 100  # matches separator width below

        formatted_date = f"{self.timestamp:%b %d, %Y %H:%M:%S}"
        customer_left = f"CUSTOMER_ID : {self.cust_id:<20}"
        if len(customer_left) + len(formatted_date) + 1 > line_width:
            customer_line = f"{customer_left} {formatted_date}"
        else:
            customer_line = f"{customer_left}{formatted_date:>{line_width - len(customer_left)}}"

        entry += (
            f"{customer_line}\n\n"
            f"{'ACCOUNT':<40} {' ' * 20} {'DEBIT':>15} {'CREDIT':>15}\n"
            f"{'-' * line_width}\n"
            f"{debit_account_with_type:<40} {self.debit.sub:>20} {formatted_debit_amount:>15} {'':>15}\n"
            f"{' ' * 4}{credit_account_with_type:<40} {self.credit.sub:>20} {'':>15} {formatted_credit_amount:>15}\n\n"
            f"{conversion_line}\n"
            f"DESCRIPTION\n{description or 'N/A'}"
            f"\n{'=' * line_width}\n"
        )
        return entry


# end of the file stop adding stuff.
