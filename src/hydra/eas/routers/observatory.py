"""Observatory router — three endpoints under ``/api/v1`` (Design §7.1, R19).

* ``GET /api/v1/observatory/latest`` — most recent
  ``exposure_posture_report`` product, wrapped in
  :class:`ExposurePostureReportResponse` (R19.4, R19.5).
* ``GET /api/v1/observatory/countries/{country_code}`` — per-country
  slice of the latest report; 404 ``NOT_FOUND`` when the code is not
  covered by the current product (R19.6).
* ``POST /api/v1/observatory/generate`` — 202 ``JobStatus``; schedules
  an on-demand regeneration of the posture report (R19.7). Expensive
  rate-limit tier and ``observatory_regenerations_per_day`` cost quota
  are wired by task 15; the router is already on the expensive path
  in that mapping.

The three endpoints all depend on :func:`get_current_tenant_id` to
enforce authentication — the observatory is tenant-agnostic for reads
per R20.5 (every tenant sees the same posture view of the world) so
the dependency is used purely for the ``X-API-Key`` validation it
triggers.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Path
from fastapi.responses import JSONResponse

from hydra.api.dependencies import get_analysis_engine, get_job_manager
from hydra.api.errors import ErrorCode, HydraAPIException, NotFoundException
from hydra.api.pagination import PaginationMeta
from hydra.api.schemas.common import APIResponse, JobStatus, ResponseMeta
from hydra.eas.dependencies import get_current_tenant_id
from hydra.eas.schemas.observatory import (
    CountryPostureResponse,
    CountryPostureSection,
    ExposurePostureReportResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["observatory"])

__all__ = ["router", "set_observatory_components", "get_observatory_generator"]


# ---------------------------------------------------------------------------
# Dependency shims — the generator singleton is wired by task 17.1.
# ---------------------------------------------------------------------------


_observatory_generator: Any | None = None


def set_observatory_components(
    *, generator: Any | None = None
) -> None:
    """Wire the :class:`ExposureObservatory` singleton (task 17.1 / tests)."""

    global _observatory_generator
    if generator is not None:
        _observatory_generator = generator


async def get_observatory_generator() -> Any | None:
    """FastAPI dependency — returns the wired :class:`ExposureObservatory`."""

    return _observatory_generator


# ---------------------------------------------------------------------------
# Validation regex (Design §4.8 / R19.6)
# ---------------------------------------------------------------------------


_COUNTRY_CODE_RE = re.compile(r"^[A-Z]{2}$")


def _empty_meta() -> ResponseMeta:
    """Minimal, pagination-free :class:`ResponseMeta`."""

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


# ---------------------------------------------------------------------------
# Product → schema adapters
# ---------------------------------------------------------------------------


def _parse_datetime(value: Any) -> datetime:
    """Best-effort coercion to an aware :class:`datetime`."""

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        iso = value.replace("Z", "+00:00") if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(iso)
        except ValueError:
            parsed = datetime.now(timezone.utc)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    return datetime.now(timezone.utc)


def _overview_countries(product: Any) -> list[dict[str, Any]]:
    """Return the list of per-country rows from the product's ``overview`` section.

    The generator writes the six R18.2 sections in a fixed order with the
    overview stored as JSON text (see
    :class:`hydra.eas.observatory.generator.ExposureObservatory._build_sections`).
    Parsing both the section id and the JSON content makes this
    resilient to a future section-order change or a shift from JSON to
    a structured column.
    """

    import json

    sections = getattr(product, "sections", None) or []
    for section in sections:
        section_id = getattr(section, "section_id", None) or (
            section.get("section_id") if isinstance(section, dict) else None
        )
        if section_id != "overview":
            continue
        content = getattr(section, "content", None) or (
            section.get("content") if isinstance(section, dict) else None
        )
        if isinstance(content, str) and content:
            try:
                parsed = json.loads(content)
            except (TypeError, ValueError):
                return []
        elif isinstance(content, dict):
            parsed = content
        else:
            return []
        countries = parsed.get("countries") if isinstance(parsed, dict) else None
        if isinstance(countries, list):
            return [c for c in countries if isinstance(c, dict)]
        return []
    return []


def _country_section_from_row(row: dict[str, Any]) -> CountryPostureSection:
    """Adapt one ``overview.countries[*]`` entry to :class:`CountryPostureSection`."""

    country_code = str(row.get("country_code") or "").upper()
    return CountryPostureSection(
        country_code=country_code,
        country_name=_country_name_for(country_code) or country_code,
        posture_score=float(row.get("posture_score") or 0.0),
        absolute_delta=float(row.get("absolute_delta") or 0.0),
        percent_delta=float(row.get("percent_delta") or 0.0),
        exposed_asset_count=int(row.get("distinct_exposed_hosts") or 0),
        critical_exposure_count=int(row.get("critical_count") or 0),
        kev_exposure_count=int(row.get("kev_count") or 0),
        top_cves=[],  # MVP: populated once we carry CVE ids through the aggregate.
    )


def _country_name_for(alpha2: str) -> str | None:
    """Resolve an ISO 3166-1 alpha-2 code to a country name if :mod:`pycountry` is available."""

    if not alpha2:
        return None
    try:
        import pycountry  # type: ignore[import-not-found]
    except ImportError:
        return None
    try:
        match = pycountry.countries.get(alpha_2=alpha2)  # type: ignore[union-attr]
    except Exception:  # noqa: BLE001
        return None
    if match is None:
        return None
    return getattr(match, "name", None)


def _latest_report_response(product: Any) -> ExposurePostureReportResponse:
    """Build an :class:`ExposurePostureReportResponse` from a raw product."""

    rows = _overview_countries(product)
    countries = [_country_section_from_row(r) for r in rows]

    # Summary + top-countries (by score DESC).
    top_sorted = sorted(countries, key=lambda s: s.posture_score, reverse=True)
    top_country_codes = [s.country_code for s in top_sorted[:10]]

    summary = getattr(product, "summary", "") or ""

    return ExposurePostureReportResponse(
        product_id=str(getattr(product, "product_id", "") or ""),
        generated_at=_parse_datetime(getattr(product, "generated_at", None)),
        countries=countries,
        summary=summary,
        top_countries_by_score=top_country_codes,
    )


# ---------------------------------------------------------------------------
# GET /api/v1/observatory/latest (R19.4, R19.5)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/observatory/latest",
    response_model=APIResponse[ExposurePostureReportResponse],
    summary="Most recent exposure posture report",
)
async def get_latest_report(
    tenant_id: UUID = Depends(get_current_tenant_id),
    engine: Any = Depends(get_analysis_engine),
) -> APIResponse[ExposurePostureReportResponse]:
    # Auth only — observatory reads are tenant-agnostic (R20.5).
    del tenant_id

    if engine is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Analysis engine is not available",
            status_code=503,
        )

    products = await engine.list_products(
        product_type="exposure_posture_report",
        limit=1,
    )
    if not products:
        raise NotFoundException("No exposure posture report has been generated yet")

    return APIResponse[ExposurePostureReportResponse](
        data=_latest_report_response(products[0]),
        meta=_empty_meta(),
    )


# ---------------------------------------------------------------------------
# GET /api/v1/observatory/countries/{country_code} (R19.6)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/observatory/countries/{country_code}",
    response_model=APIResponse[CountryPostureResponse],
    summary="Latest exposure posture for a single country",
)
async def get_country_posture(
    country_code: str = Path(..., min_length=2, max_length=2),
    tenant_id: UUID = Depends(get_current_tenant_id),
    engine: Any = Depends(get_analysis_engine),
) -> APIResponse[CountryPostureResponse]:
    del tenant_id

    # R19.6 — validate the shape before hitting the engine. The Pydantic
    # ``^[A-Z]{2}$`` regex lives on :class:`CountryPostureResponse`; we
    # mirror it here so the rejection carries the proper 422 code.
    normalized = country_code.upper()
    if not _COUNTRY_CODE_RE.match(normalized):
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message="country_code must be two uppercase ASCII letters",
            status_code=422,
        )

    if engine is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Analysis engine is not available",
            status_code=503,
        )

    products = await engine.list_products(
        product_type="exposure_posture_report",
        limit=1,
    )
    if not products:
        raise NotFoundException(f"No coverage for country {normalized}")

    product = products[0]
    rows = _overview_countries(product)
    matching_row: dict[str, Any] | None = None
    for row in rows:
        row_code = str(row.get("country_code") or "").upper()
        if row_code == normalized:
            matching_row = row
            break

    if matching_row is None:
        raise NotFoundException(f"No coverage for country {normalized}")

    section = _country_section_from_row(matching_row)
    response = CountryPostureResponse(
        country_code=normalized,
        as_of=_parse_datetime(getattr(product, "generated_at", None)),
        section=section,
    )

    return APIResponse[CountryPostureResponse](
        data=response,
        meta=_empty_meta(),
    )


# ---------------------------------------------------------------------------
# POST /api/v1/observatory/generate (R19.7, R21.2, R22.1)
# ---------------------------------------------------------------------------


@router.post(
    "/api/v1/observatory/generate",
    response_model=APIResponse[JobStatus],
    status_code=202,
    summary="Schedule an on-demand regeneration of the posture report (expensive tier)",
)
async def regenerate_report(
    tenant_id: UUID = Depends(get_current_tenant_id),
    jobs: Any = Depends(get_job_manager),
    observatory: Any | None = Depends(get_observatory_generator),
    engine: Any = Depends(get_analysis_engine),
) -> JSONResponse:
    del tenant_id

    if jobs is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Job manager is not available",
            status_code=503,
        )
    if observatory is None or engine is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Exposure observatory is not available",
            status_code=503,
        )

    job_id = await jobs.create_job()

    async def _run() -> str:
        product = await observatory.run(analysis_engine=engine)
        return product.product_id

    await jobs.run_in_background(job_id, _run())

    job = await jobs.get_job(job_id)
    if job is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Job disappeared after creation",
            status_code=503,
        )

    envelope = APIResponse[JobStatus](data=job, meta=_empty_meta())
    return JSONResponse(
        status_code=202, content=envelope.model_dump(mode="json")
    )
