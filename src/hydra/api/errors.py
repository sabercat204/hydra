"""Error codes, custom exceptions, and FastAPI exception handlers."""

from __future__ import annotations

import logging
import traceback
from enum import Enum
from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse

from hydra.api.schemas.common import APIError, APIResponse

logger = logging.getLogger(__name__)


class ErrorCode(str, Enum):
    NOT_FOUND = "NOT_FOUND"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    AUTHENTICATION_REQUIRED = "AUTHENTICATION_REQUIRED"
    FORBIDDEN = "FORBIDDEN"
    RATE_LIMITED = "RATE_LIMITED"
    CONFLICT = "CONFLICT"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    BAD_CURSOR = "BAD_CURSOR"
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    INVALID_TIME_WINDOW = "INVALID_TIME_WINDOW"
    STREAM_NOT_FOUND = "STREAM_NOT_FOUND"
    TIER_NOT_FOUND = "TIER_NOT_FOUND"
    ENTITY_REQUIRED = "ENTITY_REQUIRED"
    WATCHLIST_CONFLICT = "WATCHLIST_CONFLICT"


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class HydraAPIException(Exception):
    """Base for all API exceptions."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        detail: dict[str, Any] | None = None,
        status_code: int = 400,
    ) -> None:
        self.code = code
        self.message = message
        self.detail = detail
        self.status_code = status_code
        super().__init__(message)


class NotFoundException(HydraAPIException):
    def __init__(self, message: str = "Resource not found", detail: dict[str, Any] | None = None) -> None:
        super().__init__(ErrorCode.NOT_FOUND, message, detail, 404)


class AuthenticationError(HydraAPIException):
    def __init__(self, message: str = "Authentication required", detail: dict[str, Any] | None = None) -> None:
        super().__init__(ErrorCode.AUTHENTICATION_REQUIRED, message, detail, 401)


class ForbiddenError(HydraAPIException):
    def __init__(self, message: str = "Insufficient permissions", detail: dict[str, Any] | None = None) -> None:
        super().__init__(ErrorCode.FORBIDDEN, message, detail, 403)


class RateLimitExceeded(HydraAPIException):
    def __init__(self, retry_after: int = 60, detail: dict[str, Any] | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(ErrorCode.RATE_LIMITED, "Rate limit exceeded", detail, 429)


class ConflictError(HydraAPIException):
    def __init__(self, message: str = "Resource already exists", detail: dict[str, Any] | None = None) -> None:
        super().__init__(ErrorCode.WATCHLIST_CONFLICT, message, detail, 409)


class InvalidTimeWindowError(HydraAPIException):
    def __init__(self, message: str = "Invalid time window", detail: dict[str, Any] | None = None) -> None:
        super().__init__(ErrorCode.INVALID_TIME_WINDOW, message, detail, 422)


class EntityRequiredError(HydraAPIException):
    def __init__(self, message: str = "entity_id or entity_name required", detail: dict[str, Any] | None = None) -> None:
        super().__init__(ErrorCode.ENTITY_REQUIRED, message, detail, 422)


# ---------------------------------------------------------------------------
# Exception handlers — registered on FastAPI app
# ---------------------------------------------------------------------------

def _error_response(status_code: int, code: str, message: str, detail: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> JSONResponse:
    body = APIResponse(
        data=None,
        errors=[APIError(code=code, message=message, detail=detail)],
    ).model_dump(mode="json")
    return JSONResponse(status_code=status_code, content=body, headers=headers)


async def hydra_exception_handler(request: Request, exc: HydraAPIException) -> JSONResponse:
    headers: dict[str, str] | None = None
    if isinstance(exc, RateLimitExceeded):
        headers = {"Retry-After": str(exc.retry_after)}
    if isinstance(exc, AuthenticationError):
        headers = {"WWW-Authenticate": "ApiKey"}
    return _error_response(exc.status_code, exc.code.value, exc.message, exc.detail, headers)


async def validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    errors = exc.errors()
    detail = {"fields": [{
        "loc": list(e.get("loc", [])),
        "msg": e.get("msg", ""),
        "type": e.get("type", ""),
    } for e in errors]}
    return _error_response(422, ErrorCode.VALIDATION_ERROR.value, "Validation error", detail)


async def internal_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception: %s\n%s", exc, traceback.format_exc())
    return _error_response(500, ErrorCode.INTERNAL_ERROR.value, "Internal server error")
