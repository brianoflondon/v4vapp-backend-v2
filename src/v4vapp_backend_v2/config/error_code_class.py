from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass
class ErrorCode:
    code: Any
    start_time: datetime
    last_log_time: datetime
    message: str = ""

    def __init__(self, code: Any, message: str = ""):
        self.code = code
        self.message = message
        self.start_time = datetime.now(tz=timezone.utc)
        self.last_log_time = datetime.now(tz=timezone.utc)

        super().__init__()

    def __str__(self) -> str:
        return f"{self.code} (elapsed: {self.elapsed_time}, since last log: {self.time_since_last_log}) {self.message}"

    @property
    def code_str(self) -> str:
        return str(self.code)

    @property
    def elapsed_time(self) -> timedelta:
        return datetime.now(tz=timezone.utc) - self.start_time

    @property
    def time_since_last_log(self) -> timedelta:
        return datetime.now(tz=timezone.utc) - self.last_log_time

    def reset_last_log_time(self) -> None:
        self.last_log_time = datetime.now(tz=timezone.utc)

    def check_time_since_last_log(self, interval: timedelta | int) -> bool:
        """
        Checks if the time elapsed since the last log entry is greater than or equal to the specified interval.
        Args:
            interval (timedelta | int): The time interval to check against. If an integer, it is treated as seconds. If a timedelta, its total seconds are used.
        Returns:
            bool: True if the time since the last log is at least the interval, False otherwise.
        """

        if isinstance(interval, timedelta):
            interval_seconds = interval.total_seconds()
        else:
            interval_seconds = interval
        return self.time_since_last_log >= timedelta(seconds=interval_seconds)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the ErrorCode to a dictionary with elapsed_time and time_since_last_log as strings.
        """
        return {
            "code": self.code,
            "start_time": self.start_time.isoformat(),
            "last_log_time": self.last_log_time.isoformat(),
            "elapsed_time": str(self.elapsed_time),
            "time_since_last_log": str(self.time_since_last_log),
            "message": self.message,
        }
