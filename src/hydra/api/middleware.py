"""Middleware stack — RequestID, Timing, Rate Limiting."""

from __future__ import annotations

import hashlib
import logging
import time
import uuid
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

# Rate limit tier classification
_WRITE_PATHS = {"/products/generate", "/correlations/run"}
_WRITE_METHODS = {"POST", "DELETE"}
_SEARCH_PATHS = {"/search", "/graph", "/timeline"}


def _classify_rate_tier(path: str, method: str) -> str:
    """Classify request into rate limit tier: read, search, or write."""
    for wp in _WRITE_PATHS:
        if wp in path and method == "POST":
            return "write"
    if method in ("POST", "DELETE") and "/watchlists" in path:
        return "write"
    for sp in _SEARCH_PATHS:
        if sp in path:
            return "search"
    if "/correlations" in path and method == "GET":
        return "search"
    return "read"


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Inject X-Request-ID header on every request/response."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response


class TimingMiddleware(BaseHTTPMiddleware):
    """Measure server-side processing time."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start = time.monotonic()
        response = await call_next(request)
        duration_ms = (time.monotonic() - start) * 1000
        request.state.duration_ms = duration_ms
        response.headers["X-Process-Time"] = f"{duration_ms:.2f}"
        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Redis-backed token bucket rate limiting."""

    def __init__(self, app: Any, redis: Any = None, settings: Any = None) -> None:
        super().__init__(app)
        self._redis = redis
        self._settings = settings

    def _get_limits(self, tier: str) -> tuple[int, int]:
        """Return (rate, burst) for a tier."""
        if self._settings is None:
            return 100, 20
        s = self._settings.api
        if tier == "write":
            return s.rate_limit_write, s.rate_limit_write_burst
        if tier == "search":
            return s.rate_limit_search, s.rate_limit_search_burst
        return s.rate_limit_read, s.rate_limit_read_burst

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Skip rate limiting for ping and if disabled
        path = request.url.path
        if path.endswith("/health/ping"):
            return await call_next(request)

        if self._settings and not self._settings.api.rate_limit_enabled:
            return await call_next(request)

        api_key = request.headers.get("x-api-key", "anonymous")
        tier = _classify_rate_tier(path, request.method)
        rate, burst = self._get_limits(tier)

        remaining = rate
        reset_at = 60

        if self._redis is not None:
            key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:16]
            redis_key = f"ratelimit:{key_hash}:{tier}"
            try:
                current = await self._redis.get(redis_key)
                if current is None:
                    await self._redis.setex(redis_key, 60, str(rate - 1))
                    remaining = rate - 1
                else:
                    count = int(current)
                    if count <= 0:
                        ttl = await self._redis.ttl(redis_key)
                        response = JSONResponse(
                            status_code=429,
                            content={
                                "data": None,
                                "errors": [{"code": "RATE_LIMITED", "message": "Rate limit exceeded", "detail": None}],
                            },
                            headers={
                                "Retry-After": str(max(ttl, 1)),
                                "X-RateLimit-Limit": str(rate),
                                "X-RateLimit-Remaining": "0",
                                "X-RateLimit-Reset": str(max(ttl, 1)),
                            },
                        )
                        return response
                    await self._redis.decr(redis_key)
                    remaining = count - 1
                ttl = await self._redis.ttl(redis_key)
                reset_at = max(ttl, 1)
            except Exception:
                logger.warning("Rate limit Redis error, allowing request", exc_info=True)

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(rate)
        response.headers["X-RateLimit-Remaining"] = str(max(remaining, 0))
        response.headers["X-RateLimit-Reset"] = str(reset_at)
        return response
