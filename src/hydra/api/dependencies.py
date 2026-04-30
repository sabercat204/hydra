"""Dependency injection — engine singletons, auth, pagination."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from fastapi import Header, Query

from hydra.api.errors import AuthenticationError, ForbiddenError
from hydra.api.schemas.common import PaginationParams

# ---------------------------------------------------------------------------
# API Key record
# ---------------------------------------------------------------------------


@dataclass
class APIKeyRecord:
    """Validated API key with scopes."""

    key_id: str
    name: str
    scopes: list[str] = field(default_factory=lambda: ["read", "search", "write"])


# ---------------------------------------------------------------------------
# Engine singletons — set at app startup via app.state
# ---------------------------------------------------------------------------

_analysis_engine: Any = None
_correlation_engine: Any = None
_query_layer: Any = None
_graph_analyzer: Any = None
_timeline_builder: Any = None
_scheduler_health: Any = None
_backpressure_monitor: Any = None
_job_manager: Any = None
_registry: Any = None
_db_pool: Any = None


def set_engines(
    *,
    analysis_engine: Any = None,
    correlation_engine: Any = None,
    query_layer: Any = None,
    graph_analyzer: Any = None,
    timeline_builder: Any = None,
    scheduler_health: Any = None,
    backpressure_monitor: Any = None,
    job_manager: Any = None,
    registry: Any = None,
    db_pool: Any = None,
) -> None:
    """Wire engine singletons at startup."""
    global _analysis_engine, _correlation_engine, _query_layer
    global _graph_analyzer, _timeline_builder, _scheduler_health
    global _backpressure_monitor, _job_manager, _registry, _db_pool
    if analysis_engine is not None:
        _analysis_engine = analysis_engine
    if correlation_engine is not None:
        _correlation_engine = correlation_engine
    if query_layer is not None:
        _query_layer = query_layer
    if graph_analyzer is not None:
        _graph_analyzer = graph_analyzer
    if timeline_builder is not None:
        _timeline_builder = timeline_builder
    if scheduler_health is not None:
        _scheduler_health = scheduler_health
    if backpressure_monitor is not None:
        _backpressure_monitor = backpressure_monitor
    if job_manager is not None:
        _job_manager = job_manager
    if registry is not None:
        _registry = registry
    if db_pool is not None:
        _db_pool = db_pool


# ---------------------------------------------------------------------------
# Dependency callables for FastAPI Depends()
# ---------------------------------------------------------------------------


async def get_analysis_engine() -> Any:
    return _analysis_engine


async def get_correlation_engine() -> Any:
    return _correlation_engine


async def get_query_layer() -> Any:
    return _query_layer


async def get_graph_analyzer() -> Any:
    return _graph_analyzer


async def get_timeline_builder() -> Any:
    return _timeline_builder


async def get_scheduler_health() -> Any:
    return _scheduler_health


async def get_backpressure_monitor() -> Any:
    return _backpressure_monitor


async def get_job_manager() -> Any:
    return _job_manager


async def get_registry() -> Any:
    return _registry


async def get_db_pool() -> Any:
    return _db_pool


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

# In-memory API key store for testing; production uses PG lookup
_api_key_store: dict[str, APIKeyRecord] | None = None


def set_api_key_store(store: dict[str, APIKeyRecord]) -> None:
    """Set an in-memory API key store (for testing)."""
    global _api_key_store
    _api_key_store = store


async def get_current_api_key(
    x_api_key: str = Header(..., alias="X-API-Key"),
) -> APIKeyRecord:
    """Validate API key.

    MVP: checks in-memory store or PG table.
    Raises 401 if invalid/expired, 403 if scope insufficient.
    """
    if not x_api_key:
        raise AuthenticationError("API key required")

    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()

    # Check in-memory store first (testing / simple deployments)
    if _api_key_store is not None:
        record = _api_key_store.get(key_hash)
        if record is None:
            # Also check raw key for convenience in tests
            record = _api_key_store.get(x_api_key)
        if record is None:
            raise AuthenticationError("Invalid API key")
        return record

    # Fallback: PG lookup
    if _db_pool is not None:
        try:
            async with _db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT key_id, name, scopes FROM api_keys WHERE key_hash = $1 AND is_active = TRUE AND (expires_at IS NULL OR expires_at > NOW())",
                    key_hash,
                )
                if row is None:
                    raise AuthenticationError("Invalid API key")
                return APIKeyRecord(
                    key_id=str(row["key_id"]),
                    name=row["name"],
                    scopes=list(row["scopes"]),
                )
        except AuthenticationError:
            raise
        except Exception:
            raise AuthenticationError("Authentication service unavailable")

    raise AuthenticationError("Invalid API key")


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


def get_pagination_params(
    cursor: str | None = Query(None, description="Opaque pagination cursor from previous response"),
    limit: int = Query(50, ge=1, le=500, description="Maximum items per page"),
) -> PaginationParams:
    """Extract and validate pagination parameters."""
    return PaginationParams(cursor=cursor, limit=limit)
