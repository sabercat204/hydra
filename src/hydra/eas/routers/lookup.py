"""Lookup router — ``GET /api/v1/lookup/{indicator}`` (Design §7.1, §2.3, R16/R17).

Single endpoint. Contract:

1. Classify the URL ``{indicator}`` segment. ``None`` → 422
   ``INDICATOR_NOT_CLASSIFIED`` (R16.1).
2. Normalize to the canonical cache-key form (R16.2–R16.4).
3. ``GET hydra:eas:lookup:{cls}:{normalized_value}`` from the cache DB.
4. **Cache hit** — decode the msgpack blob, merge the per-tenant
   ``asset_reference`` (see R17.5), return with ``meta.cache = "hit"``.
5. **Cache miss** — acquire the single-flight lock. If acquired, run
   :meth:`LookupAssembler.assemble`, write the shared (non-tenant)
   payload to the cache, return with ``meta.cache = "miss"``. If we
   lost the flight, poll the cache for up to 2 s; on success return
   the cached payload with ``meta.cache = "hit"``. On timeout fall
   through to an uncached assembly (Design §3.7 safety valve).

The ``asset_reference`` field is layered on **per request** after the
cache read because it's the only tenant-scoped field in
:class:`LookupResponse` — serving it from the cache would either leak
across tenants or break the cache's shareability (R17.5, Property 21).

Cost-quota enforcement and expensive-tier rate-limit wiring live in task
15 (``enforce_cost_quota``). The router here focuses on the classify →
normalize → cache-or-assemble → compose flow.
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Path
from fastapi.responses import JSONResponse

from hydra.api.errors import ErrorCode, HydraAPIException
from hydra.api.pagination import PaginationMeta
from hydra.api.schemas.common import APIResponse, ResponseMeta
from hydra.eas.dependencies import get_current_tenant_id
from hydra.eas.lookup.assembler import LookupAssembler
from hydra.eas.lookup.cache import (
    IndicatorLookupCache,
    decode_payload,
    encode_payload,
)
from hydra.eas.lookup.classifier import classify_indicator
from hydra.eas.lookup.normalizer import normalize_indicator
from hydra.eas.lookup.singleflight import SingleFlightLock, cache_key
from hydra.eas.schemas.lookup import (
    IndicatorClass,
    LookupAssetReference,
    LookupResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["lookup"])

__all__ = ["router"]


# ---------------------------------------------------------------------------
# Dependency shims
#
# The lookup capability has three distinct singletons — the cache, the
# single-flight lock, and the assembler. They're wired by ``setup_eas``
# (task 17.1). Until that lands the getters return ``None`` and the
# router surfaces a 503 rather than crashing. We deliberately mirror
# the pattern used in ``src/hydra/eas/dependencies.py`` instead of
# growing that module further: the lookup singletons are not used by
# any other router, so there's no cross-capability coupling.
# ---------------------------------------------------------------------------


_lookup_cache: IndicatorLookupCache | None = None
_lookup_singleflight: SingleFlightLock | None = None
_lookup_assembler: LookupAssembler | None = None
_singleflight_ttl_seconds: int = 10
_singleflight_wait_timeout_ms: int = 2_000


def set_lookup_components(
    *,
    cache: IndicatorLookupCache | None = None,
    singleflight: SingleFlightLock | None = None,
    assembler: LookupAssembler | None = None,
    singleflight_ttl_seconds: int | None = None,
    singleflight_wait_timeout_ms: int | None = None,
) -> None:
    """Wire the lookup singletons (task 17.1 + test fixtures).

    Only non-``None`` arguments are installed, so partial overrides in
    tests don't clobber previously-set singletons.
    """

    global _lookup_cache, _lookup_singleflight, _lookup_assembler
    global _singleflight_ttl_seconds, _singleflight_wait_timeout_ms
    if cache is not None:
        _lookup_cache = cache
    if singleflight is not None:
        _lookup_singleflight = singleflight
    if assembler is not None:
        _lookup_assembler = assembler
    if singleflight_ttl_seconds is not None:
        _singleflight_ttl_seconds = int(singleflight_ttl_seconds)
    if singleflight_wait_timeout_ms is not None:
        _singleflight_wait_timeout_ms = int(singleflight_wait_timeout_ms)


async def get_lookup_cache() -> IndicatorLookupCache | None:
    return _lookup_cache


async def get_lookup_singleflight() -> SingleFlightLock | None:
    return _lookup_singleflight


async def get_lookup_assembler() -> LookupAssembler | None:
    return _lookup_assembler


# ---------------------------------------------------------------------------
# Meta helpers
# ---------------------------------------------------------------------------


def _lookup_meta() -> ResponseMeta:
    """Minimal ``ResponseMeta`` — app middleware fills request_id / timestamp."""

    return ResponseMeta(
        request_id="",
        timestamp="",
        duration_ms=0.0,
        pagination=PaginationMeta(
            next_cursor=None,
            has_more=False,
            total_estimate=None,
        ),
    )


def _compose_body(
    envelope: APIResponse[LookupResponse],
    cache_status: str,
) -> dict[str, Any]:
    """Serialize the response envelope and stamp ``meta.cache``."""

    body = envelope.model_dump(mode="json")
    meta = body.get("meta") or {}
    meta["cache"] = cache_status
    body["meta"] = meta
    return body


# ---------------------------------------------------------------------------
# GET /api/v1/lookup/{indicator}
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/lookup/{indicator}",
    response_model=APIResponse[LookupResponse],
    summary="Fast single-indicator lookup — cache-first with single-flight on miss",
)
async def lookup_indicator(
    indicator: str = Path(..., min_length=1, max_length=512),
    tenant_id: UUID = Depends(get_current_tenant_id),
    cache: IndicatorLookupCache | None = Depends(get_lookup_cache),
    singleflight: SingleFlightLock | None = Depends(get_lookup_singleflight),
    assembler: LookupAssembler | None = Depends(get_lookup_assembler),
) -> JSONResponse:
    """Resolve everything HYDRA knows about ``indicator`` in a single call.

    The flow mirrors Design §2.3 verbatim. See the module docstring for
    the contract overview.
    """

    # ---- Classify + normalize ----------------------------------------
    # The classifier accepts the raw URL path segment; we then normalize
    # via the per-class rules so that the cache key, asset lookup, and
    # fan-out queries all agree on one canonical form.
    cls = classify_indicator(indicator)
    if cls is None:
        raise HydraAPIException(
            code=ErrorCode.INDICATOR_NOT_CLASSIFIED,
            message=f"Could not classify indicator: {indicator!r}",
            detail={"indicator": indicator},
            status_code=422,
        )

    try:
        normalized_value = normalize_indicator(cls, indicator)
    except ValueError as exc:
        # Classification succeeded but normalization refused — typically
        # a malformed IDNA label or a hash with the wrong length after
        # lowercasing. Map to the same 422 code so clients can
        # differentiate from generic validation errors.
        raise HydraAPIException(
            code=ErrorCode.INDICATOR_NOT_CLASSIFIED,
            message=str(exc),
            detail={"indicator": indicator, "indicator_class": cls},
            status_code=422,
        ) from exc

    # ---- Service-availability guard ----------------------------------
    if cache is None or singleflight is None or assembler is None:
        # The router can't serve without its dependencies; this is a
        # 503 rather than a 500 because the condition is recoverable
        # (startup still in progress, or a transient wiring failure).
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Indicator lookup service is not available",
            status_code=503,
        )

    # ---- Cache hit path ----------------------------------------------
    cached_bytes = await cache.get(cls, normalized_value)
    if cached_bytes is not None:
        payload = _decode_cached_payload(cached_bytes, cls, normalized_value)
        if payload is not None:
            asset_ref = await _resolve_asset_reference(
                assembler, cls, normalized_value, tenant_id
            )
            payload.asset_reference = asset_ref
            envelope = APIResponse[LookupResponse](
                data=payload, meta=_lookup_meta()
            )
            return JSONResponse(status_code=200, content=_compose_body(envelope, "hit"))
        # decode failed — fall through to the miss path so we don't
        # serve garbage from a corrupted cache entry.

    # ---- Cache miss: single-flight ----------------------------------
    request_id = secrets.token_hex(8)
    acquired = await singleflight.acquire(
        cls, normalized_value, request_id, ttl_seconds=_singleflight_ttl_seconds
    )

    if acquired:
        try:
            payload = await assembler.assemble(cls, normalized_value, tenant_id)
            # Cache the **shared** view (R17.5): strip the tenant-scoped
            # asset_reference before encoding. The router re-attaches
            # per-request on both hit and miss paths so the response the
            # caller sees always includes it when appropriate.
            shared = payload.model_copy(update={"asset_reference": None})
            try:
                encoded = encode_payload(shared.model_dump(mode="json"))
                await cache.set(cls, normalized_value, encoded)
            except Exception as exc:  # noqa: BLE001
                # Cache-write failure is non-fatal: log and continue.
                logger.warning(
                    "eas.lookup.router.cache_write_failed",
                    extra={
                        "indicator_class": cls,
                        "indicator": normalized_value,
                        "error": str(exc),
                    },
                )
        finally:
            await singleflight.release(cls, normalized_value, request_id)

        envelope = APIResponse[LookupResponse](data=payload, meta=_lookup_meta())
        return JSONResponse(status_code=200, content=_compose_body(envelope, "miss"))

    # ---- Lost the flight: poll the cache ----------------------------
    key = cache_key(cls, normalized_value)
    waited = await singleflight.wait_for_value(
        key,
        poll_interval_ms=50,
        timeout_ms=_singleflight_wait_timeout_ms,
    )
    if waited is not None:
        payload = _decode_cached_payload(waited, cls, normalized_value)
        if payload is not None:
            asset_ref = await _resolve_asset_reference(
                assembler, cls, normalized_value, tenant_id
            )
            payload.asset_reference = asset_ref
            envelope = APIResponse[LookupResponse](
                data=payload, meta=_lookup_meta()
            )
            return JSONResponse(status_code=200, content=_compose_body(envelope, "hit"))

    # ---- Safety valve: uncached assemble ----------------------------
    # We lost the flight AND the winner didn't publish within 2 s. Per
    # Design §3.7 we fall through to an uncached assembly rather than
    # return an error — under normal load this branch is unreachable.
    logger.warning(
        "eas.lookup.router.safety_valve",
        extra={"indicator_class": cls, "indicator": normalized_value},
    )
    payload = await assembler.assemble(cls, normalized_value, tenant_id)
    envelope = APIResponse[LookupResponse](data=payload, meta=_lookup_meta())
    return JSONResponse(status_code=200, content=_compose_body(envelope, "miss"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decode_cached_payload(
    raw: bytes,
    cls: IndicatorClass,
    normalized_value: str,
) -> LookupResponse | None:
    """Decode a msgpack blob into :class:`LookupResponse`, or ``None`` on failure.

    A decode failure means the cache entry is corrupted or was written
    by an incompatible schema version. We log and return ``None`` so
    the router falls through to the miss path and re-populates.
    """

    try:
        decoded = decode_payload(raw)
    except Exception as exc:  # noqa: BLE001 — any decode issue
        logger.warning(
            "eas.lookup.router.cache_decode_failed",
            extra={
                "indicator_class": cls,
                "indicator": normalized_value,
                "error": str(exc),
            },
        )
        return None

    if not isinstance(decoded, dict):
        return None

    # Cached payload was stored without ``asset_reference`` — we need
    # to re-hydrate timestamps that msgpack/JSON round-trips preserve
    # as strings. Pydantic handles that automatically on
    # ``model_validate``.
    decoded.pop("asset_reference", None)
    try:
        return LookupResponse.model_validate(decoded)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "eas.lookup.router.cache_validate_failed",
            extra={
                "indicator_class": cls,
                "indicator": normalized_value,
                "error": str(exc),
            },
        )
        return None


async def _resolve_asset_reference(
    assembler: LookupAssembler,
    cls: IndicatorClass,
    normalized_value: str,
    tenant_id: UUID,
) -> LookupAssetReference | None:
    """Per-request tenant-scoped asset_reference lookup (R17.5).

    The assembler exposes the internal ``_fetch_asset_reference``
    coroutine; we call it here on cache hits so the cached payload
    doesn't carry another tenant's data.
    """

    fetch = getattr(assembler, "_fetch_asset_reference", None)
    if fetch is None:  # pragma: no cover - defensive
        return None
    try:
        return await fetch(cls, normalized_value, tenant_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "eas.lookup.router.asset_resolve_failed",
            extra={
                "indicator_class": cls,
                "indicator": normalized_value,
                "error": str(exc),
            },
        )
        return None


# ---------------------------------------------------------------------------
# Debug helper — unused in the response flow but kept for symmetry with
# other routers that expose ``_now_iso`` for their meta populate step.
# ---------------------------------------------------------------------------


def _now_iso() -> str:  # pragma: no cover - trivial
    return datetime.now(timezone.utc).isoformat()
