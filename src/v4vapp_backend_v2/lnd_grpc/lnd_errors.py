from grpc import StatusCode


class LNDConnectionError(Exception):
    pass


class LNDStartupError(LNDConnectionError):
    pass


class LNDSubscriptionError(LNDConnectionError):
    pass

    def __init__(
        self,
        message: str,
        rpc_error_code: StatusCode,
        rpc_error_details: str,
        call_name: str,
        original_error: Exception,
    ) -> None:
        super().__init__(message)
        self.rpc_error_code = rpc_error_code
        self.rpc_error_details = rpc_error_details
        self.call_name = call_name
        self.original_error = original_error
