import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from v4vapp_backend_v2.config.setup import logger


class ApiError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        extra: dict | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        self.extra = extra or {}
        super().__init__(message)

    def body(self) -> dict:
        payload: dict = {"error": {"code": self.code, "message": self.message}}
        payload.update(self.extra)
        return payload


def _api_error_extra(request: Request, exc: ApiError) -> dict:
    extra: dict = {
        "method": request.method,
        "path": request.url.path,
        "status": exc.status_code,
        "error_code": exc.code,
    }
    for key in ("invoice_id", "external_id", "cust_id"):
        val = getattr(request.state, key, None)
        if val is not None:
            extra[key] = val
    if "invoice_id" in request.path_params:
        extra.setdefault("invoice_id", request.path_params["invoice_id"])
    if "external_id" in request.path_params:
        extra.setdefault("external_id", request.path_params["external_id"])
    return extra


def register_dash_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
        extra = _api_error_extra(request, exc)
        if exc.status_code >= 500:
            level = logging.ERROR
        else:
            level = logging.WARNING
        logger.log(level, "dash api error", extra=extra)
        return JSONResponse(status_code=exc.status_code, content=exc.body())
