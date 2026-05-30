"""Images router — screenshot retrieval, similarity search, capture (Design §7.1).

Three endpoints under ``/api/v1``:

* ``GET /api/v1/images/{record_hash}`` — stream the PNG from MinIO, or
  return the :class:`ImageMetadataResponse` when ``metadata_only=true``.
  404 ``NOT_FOUND`` when the record is unknown; 503 ``BLOB_UNAVAILABLE``
  when the record exists but the MinIO object is missing (R7.3, R7.4).
* ``GET /api/v1/images/search`` — perceptual-hash similarity search. The
  phash is validated as 16-char lowercase hex (422 on failure, R8.2).
  Filtering by ``tiers``, ``since``, and ``url_contains`` is applied
  before similarity scoring; results are capped at
  ``EASSettings.images_search_max_results`` (R8.4).
* ``POST /api/v1/assets/{asset_id}/screenshot?url=...`` — 202-accepted
  job that enqueues the URL onto the screenshot worker queue. Uses the
  shared :class:`hydra.api.jobs.JobManager` for status tracking. The
  expensive-tier rate limiter and cost quota are wired by task 15
  (they depend on ``RateLimitMiddleware`` + ``enforce_cost_quota``);
  for now the endpoint only depends on ``get_current_tenant_id``.

Similarity search implementation note. The design (§4.11) prescribes a
``script_score`` query that Hamming-distances the binary ``phash_bits``
field. For the MVP we use a simpler two-step strategy that keeps the
query index-friendly and client-portable:

1. Pull candidate docs from the ``hydra-screenshots`` index using only
   the keyword / date / text filters.
2. Compute the Hamming similarity in Python against every candidate,
   sort, trim to :attr:`EASSettings.images_search_max_results`, and
   page.

This is acceptable because the response is bounded by
``images_search_max_results`` (default 500) and the index filter does
the heavy lifting. A pure ES ``script_score`` path can replace this
implementation in a follow-up task when the dataset grows.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Response
from fastapi.responses import JSONResponse, StreamingResponse

from hydra.api.dependencies import get_job_manager
from hydra.api.errors import ErrorCode, HydraAPIException, NotFoundException
from hydra.api.pagination import PaginationMeta
from hydra.api.schemas.common import APIResponse, JobStatus, ResponseMeta
from hydra.eas.dependencies import (
    get_current_tenant_id,
    get_eas_redis,
    get_es_client,
    get_minio_client,
)
from hydra.eas.schemas.images import (
    ImageMetadataResponse,
    ImageSearchParams,
    ImageSearchResult,
)
from hydra.eas.screenshots.phash import hamming_similarity
from hydra.eas.screenshots.worker import SCREENSHOT_QUEUE_KEY
from hydra.eas.storage.es_mappings import HYDRA_SCREENSHOTS_INDEX

logger = logging.getLogger(__name__)

router = APIRouter(tags=["images"])

__all__ = ["router"]


_PHASH_RE = re.compile(r"^[0-9a-f]{16}$")
_RECORD_HASH_RE = re.compile(r"^[0-9a-f]{16}$")


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _empty_meta(next_cursor: str | None = None) -> ResponseMeta:
    """Minimal ``ResponseMeta`` with pagination slot populated.

    The app-wide middleware fills ``request_id`` / ``duration_ms`` /
    ``timestamp`` before the response leaves the process.
    """

    return ResponseMeta(
        request_id="",
        timestamp="",
        duration_ms=0.0,
        pagination=PaginationMeta(
            next_cursor=next_cursor,
            has_more=next_cursor is not None,
            total_estimate=None,
        ),
    )


def _coerce_datetime(value: Any) -> datetime:
    """Coerce an ES ``date`` value into an aware :class:`datetime`.

    ES returns ISO strings for ``date`` fields when the default
    deserializer is in use. The ``elasticsearch`` python client keeps
    them as strings unless the user configures a custom serializer.
    """

    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        # Tolerate a trailing ``Z`` (ES often emits UTC as ``...Z``).
        iso = value.replace("Z", "+00:00") if value.endswith("Z") else value
        try:
            return datetime.fromisoformat(iso)
        except ValueError:
            return datetime.fromtimestamp(0)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value) / 1000.0)
    return datetime.fromtimestamp(0)


def _doc_to_metadata(doc: dict[str, Any]) -> ImageMetadataResponse:
    """Adapt a raw ES document to :class:`ImageMetadataResponse`.

    We only read fields we know exist in the mapping (Design §4.11) and
    tolerate missing optional fields — some old records may predate a
    mapping extension.
    """

    viewport_w = int(doc.get("viewport_w") or 1280)
    viewport_h = int(doc.get("viewport_h") or 800)
    return ImageMetadataResponse(
        record_hash=str(doc["record_hash"]),
        url=str(doc.get("url") or ""),
        http_status=(
            int(doc["http_status"])
            if doc.get("http_status") is not None
            else None
        ),
        title=doc.get("title"),
        phash=str(doc["phash"]),
        content_hash=str(doc["content_hash"]),
        rendered_at=_coerce_datetime(doc.get("rendered_at")),
        viewport=(viewport_w, viewport_h),
        minio_key=str(doc.get("minio_key") or ""),
        has_ocr=bool(doc.get("ocr_text")),
        ocr_excerpt=doc.get("ocr_excerpt"),
    )


async def _fetch_screenshot_doc(
    es: Any, record_hash: str
) -> dict[str, Any] | None:
    """Return the ES ``hydra-screenshots`` doc for ``record_hash`` or None."""

    try:
        resp = await es.get(index=HYDRA_SCREENSHOTS_INDEX, id=record_hash)
    except Exception as exc:  # noqa: BLE001 — ES-404 surfaces as exception
        message = str(exc).lower()
        if "not_found" in message or "404" in message:
            return None
        logger.warning(
            "eas.images.es_get_failed",
            extra={"record_hash": record_hash, "error": str(exc)},
        )
        return None

    # The 8.x client wraps the body; support both ``.body`` and dict access.
    body = getattr(resp, "body", None) or resp
    if not isinstance(body, dict):
        return None
    source = body.get("_source")
    if not isinstance(source, dict):
        return None
    return source


# ----------------------------------------------------------------------
# GET /api/v1/images/{record_hash} — single record (R7.1–R7.4)
# ----------------------------------------------------------------------


@router.get(
    "/api/v1/images/{record_hash}",
    summary="Stream a screenshot PNG, or return its metadata",
)
async def get_image(
    record_hash: Annotated[
        str,
        Path(pattern=r"^[0-9a-f]{16}$", description="xxhash64 record hash"),
    ],
    metadata_only: Annotated[bool, Query()] = False,
    tenant_id: UUID = Depends(get_current_tenant_id),
    es: Any = Depends(get_es_client),
    minio: Any = Depends(get_minio_client),
) -> Response:
    _ = tenant_id  # screenshots are tenant-agnostic for reads (R20.5)
    if not _RECORD_HASH_RE.match(record_hash):
        # Defence in depth — FastAPI's ``Path(pattern=...)`` already rejects
        # malformed hashes with 422, but a future signature change
        # shouldn't silently skip the validation.
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message="record_hash must be 16 lowercase hex chars",
            status_code=422,
        )

    if es is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Image index is not available",
            status_code=503,
        )

    doc = await _fetch_screenshot_doc(es, record_hash)
    if doc is None:
        raise NotFoundException(f"Screenshot {record_hash} not found")

    if metadata_only:
        envelope = APIResponse[ImageMetadataResponse](
            data=_doc_to_metadata(doc), meta=_empty_meta()
        )
        return JSONResponse(
            status_code=200, content=envelope.model_dump(mode="json")
        )

    # Stream the PNG from MinIO.
    if minio is None:
        raise HydraAPIException(
            code=ErrorCode.BLOB_UNAVAILABLE,
            message="Blob store is not available",
            status_code=503,
        )

    minio_key = str(doc.get("minio_key") or "")
    bucket = "hydra-screenshots"
    object_key = minio_key
    if object_key.startswith(f"{bucket}/"):
        object_key = object_key[len(bucket) + 1 :]

    try:
        # ``minio_client.get_object`` in boto3 is synchronous; we call it
        # on the event loop and rely on the thread pool for decompression.
        # Tests that inject an async fake are supported via the inspect
        # branch below.
        import asyncio
        import inspect

        get_obj = minio.get_object
        if inspect.iscoroutinefunction(get_obj):
            resp_obj = await get_obj(Bucket=bucket, Key=object_key)
        else:
            resp_obj = await asyncio.to_thread(
                get_obj, Bucket=bucket, Key=object_key
            )
    except Exception as exc:  # noqa: BLE001 — MinIO miss path (R7.4)
        logger.info(
            "eas.images.blob_unavailable",
            extra={
                "record_hash": record_hash,
                "minio_key": minio_key,
                "error": str(exc),
            },
        )
        raise HydraAPIException(
            code=ErrorCode.BLOB_UNAVAILABLE,
            message="Screenshot blob is missing from MinIO",
            detail={"record_hash": record_hash, "minio_key": minio_key},
            status_code=503,
        )

    body = resp_obj.get("Body") if isinstance(resp_obj, dict) else None

    if body is None:
        raise HydraAPIException(
            code=ErrorCode.BLOB_UNAVAILABLE,
            message="Screenshot blob has no body",
            status_code=503,
        )

    # ``Body`` is typically a botocore StreamingBody; read into a single
    # chunk for now — the MVP MinIO settings cap uploads at 20 MB so a
    # full read is safe. A future optimisation is to stream in chunks.
    try:
        payload = body.read()
    except Exception as exc:  # noqa: BLE001
        raise HydraAPIException(
            code=ErrorCode.BLOB_UNAVAILABLE,
            message="Screenshot blob read failed",
            detail={"error": str(exc)},
            status_code=503,
        )

    async def _iter() -> Any:
        yield payload

    return StreamingResponse(
        _iter(),
        media_type="image/png",
        headers={
            "Content-Length": str(len(payload)),
            "X-Record-Hash": record_hash,
        },
    )


# ----------------------------------------------------------------------
# GET /api/v1/images/search — phash similarity (R8)
# ----------------------------------------------------------------------


@router.get(
    "/api/v1/images/search",
    response_model=APIResponse[list[ImageSearchResult]],
    summary="Perceptual-hash similarity search",
)
async def search_images(
    phash: Annotated[str, Query(description="16-char lowercase hex pHash")],
    similarity: Annotated[float, Query(ge=0.0, le=1.0)] = 0.85,
    tiers: Annotated[list[int] | None, Query()] = None,
    since: Annotated[datetime | None, Query()] = None,
    url_contains: Annotated[str | None, Query(max_length=256)] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    tenant_id: UUID = Depends(get_current_tenant_id),
    es: Any = Depends(get_es_client),
) -> APIResponse[list[ImageSearchResult]]:
    _ = tenant_id  # tenant-agnostic read surface (R20.5)

    # Validate the phash format **before** building or parsing anything
    # else (R8.2: MUST NOT execute any database query on malformed input).
    if not _PHASH_RE.match(phash):
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message="phash must be 16 lowercase hex characters",
            status_code=422,
        )
    # Reuse the Pydantic params model for normalization & defensive
    # re-validation of the full parameter set.
    params = ImageSearchParams(
        phash=phash,
        similarity=similarity,
        tiers=tiers,
        since=since,
        url_contains=url_contains,
    )

    if es is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Image index is not available",
            status_code=503,
        )

    # Pull the raw EAS settings to honour ``images_search_max_results``
    # (R8.4 hard cap regardless of the caller's ``limit``).
    from hydra.eas.dependencies import get_eas_settings

    hydra_settings = await get_eas_settings()
    cap = int(hydra_settings.eas.images_search_max_results)
    effective_limit = min(limit, cap)

    # Build an ES query with the filter-only clause (no similarity
    # scoring — we do that in Python per the design note above).
    filters: list[dict[str, Any]] = []
    if params.tiers:
        filters.append({"terms": {"tier": [int(t) for t in params.tiers]}})
    if params.since is not None:
        filters.append({"range": {"rendered_at": {"gte": params.since.isoformat()}}})
    if params.url_contains:
        filters.append(
            {"wildcard": {"url": {"value": f"*{params.url_contains}*"}}}
        )

    query: dict[str, Any]
    if filters:
        query = {"bool": {"filter": filters}}
    else:
        query = {"match_all": {}}

    # Fetch up to ``cap`` candidates — enough to produce ``effective_limit``
    # hits after Python-side similarity filtering.
    try:
        raw = await es.search(
            index=HYDRA_SCREENSHOTS_INDEX,
            query=query,
            size=cap,
            sort=[{"rendered_at": {"order": "desc"}}],
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "eas.images.search_failed",
            extra={"phash": params.phash, "error": str(exc)},
        )
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Image search backend unavailable",
            status_code=503,
        )

    body = getattr(raw, "body", None) or raw
    hits = (
        body.get("hits", {}).get("hits", [])
        if isinstance(body, dict)
        else []
    )

    scored: list[ImageSearchResult] = []
    for hit in hits:
        source = hit.get("_source") if isinstance(hit, dict) else None
        if not isinstance(source, dict):
            continue
        doc_phash = source.get("phash")
        if not isinstance(doc_phash, str) or not _PHASH_RE.match(doc_phash):
            continue
        try:
            sim = hamming_similarity(params.phash, doc_phash)
        except ValueError:
            continue
        if sim < params.similarity:
            continue
        scored.append(
            ImageSearchResult(
                record_hash=str(source.get("record_hash") or hit.get("_id") or ""),
                url=str(source.get("url") or ""),
                phash=doc_phash,
                similarity=sim,
                rendered_at=_coerce_datetime(source.get("rendered_at")),
                title=source.get("title"),
            )
        )

    # Sort by similarity DESC (primary) then record_hash ASC (tiebreak)
    # so the order is stable across repeated queries.
    scored.sort(key=lambda r: (-r.similarity, r.record_hash))
    trimmed = scored[:effective_limit]

    return APIResponse[list[ImageSearchResult]](
        data=trimmed, meta=_empty_meta()
    )


# ----------------------------------------------------------------------
# POST /api/v1/assets/{asset_id}/screenshot — on-demand capture (R6, R21.2)
# ----------------------------------------------------------------------


@router.post(
    "/api/v1/assets/{asset_id}/screenshot",
    response_model=APIResponse[JobStatus],
    status_code=202,
    summary="Queue an on-demand screenshot capture (expensive tier)",
)
async def capture_asset_screenshot(
    asset_id: UUID,
    url: Annotated[str, Query(min_length=1, max_length=2048)],
    tenant_id: UUID = Depends(get_current_tenant_id),
    jobs: Any = Depends(get_job_manager),
    redis: Any = Depends(get_eas_redis),
) -> JSONResponse:
    """Enqueue a screenshot render for ``url`` and return a ``JobStatus``.

    The expensive-tier rate limit (R21.2) and the cost quota (R22.1) are
    installed by task 15 via middleware + ``enforce_cost_quota``
    dependency; this endpoint is already wired to the expensive path in
    that mapping. Until task 15 lands, this route still returns 202 but
    without quota enforcement.
    """

    if jobs is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Job manager is not available",
            status_code=503,
        )
    if redis is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Screenshot queue is not available",
            status_code=503,
        )

    job_id = await jobs.create_job()

    # Enqueue the entry. We do NOT render inline — the worker pool owns
    # the render path and the rate-limit / concurrency story.
    import json as _json

    payload = _json.dumps(
        {
            "url": url,
            "tenant_id": str(tenant_id),
            "source": "explicit",
            "asset_id": str(asset_id),
            "job_id": job_id,
        }
    )
    try:
        await redis.rpush(SCREENSHOT_QUEUE_KEY, payload)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "eas.images.enqueue_failed",
            extra={"url": url, "asset_id": str(asset_id), "error": str(exc)},
        )
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Failed to enqueue screenshot job",
            status_code=503,
        )

    job = await jobs.get_job(job_id)
    if job is None:
        # Should not happen — ``create_job`` wrote the record — but guard
        # against a Redis outage between the two calls.
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Job disappeared after creation",
            status_code=503,
        )

    envelope = APIResponse[JobStatus](data=job, meta=_empty_meta())
    return JSONResponse(
        status_code=202, content=envelope.model_dump(mode="json")
    )
