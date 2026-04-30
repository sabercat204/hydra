"""Adapter exception hierarchy."""

from __future__ import annotations


class AdapterError(Exception):
    """Base exception for all adapter failures."""


class FetchError(AdapterError):
    """Upstream unreachable, timeout, or non-retryable HTTP error."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        self.status_code = status_code
        super().__init__(message)


class RateLimitError(FetchError):
    """HTTP 429 or equivalent rate-limit response."""

    def __init__(self, message: str, retry_after: float = 1.0, status_code: int = 429) -> None:
        self.retry_after = max(retry_after, 1.0)
        super().__init__(message, status_code=status_code)


class ParseError(AdapterError):
    """Response body cannot be parsed into expected structure."""


class ValidationError(AdapterError):
    """Record fails validation rules."""


class AdapterRegistryMismatch(AdapterError):
    """Stream registry declares a different adapter type than the instantiated subclass."""
