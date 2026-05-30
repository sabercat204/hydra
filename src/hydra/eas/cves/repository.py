"""Read-side helpers for CVE, exploit, and affected-asset queries (R11).

Four methods live on :class:`CVERepository`:

* :meth:`get_cve_detail` — fetches the latest NVD / EPSS / KEV docs for a
  CVE from the ``hydra-cves`` ES index and merges them into a single
  :class:`CVEDetailResponse` (R11.1).
* :meth:`search_cves` — paged search across the ``hydra-cves`` index
  with vendor / product / min_cvss / kev_only / published filters
  (R11.3). Cursor pagination on ``(published DESC, cve_id)``.
* :meth:`search_exploits` — paged search across ``hydra-cves`` docs
  whose ``source`` is ``exploitdb`` or ``metasploit`` (R11.5).
* :meth:`list_affected_assets` — PG join of ``correlation_results`` and
  ``asset_exposures`` to return the tenant-owned assets affected by a
  given CVE (R11.4).

The ES calls use the async 8.x client shape ``es.search(index=...,
query=..., size=..., sort=...)`` and tolerate either the
:class:`elasticsearch.ObjectApiResponse` wrapper (``.body`` attribute)
or a plain ``dict`` returned by test doubles.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from hydra.api.pagination import decode_cursor, encode_cursor
from hydra.eas.assets.models import Asset
from hydra.eas.schemas.cves import (
    CVEDetailResponse,
    CVESearchParams,
    CVESearchResult,
    ExploitSearchResult,
)
from hydra.eas.storage.es_mappings import HYDRA_CVES_INDEX

logger = logging.getLogger(__name__)

__all__ = ["CVERepository", "ExploitSearchParams"]


# Parameter dataclass used by the exploit-search helper. We keep this
# here rather than in ``schemas/cves.py`` because it's an internal
# repository contract — the exploit router unpacks query params directly
# when calling the repo method.
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class ExploitSearchParams:
    """Filter set for :meth:`CVERepository.search_exploits` (R11.5)."""

    cve_id: str | None = None
    platform: str | None = None
    type: str | None = None
    published_after: datetime | None = None


# ---------------------------------------------------------------------------
# Helpers shared across methods
# ---------------------------------------------------------------------------


def _unwrap_es_response(resp: Any) -> dict[str, Any]:
    """Return a plain dict from an ES-client response.

    Works with both the 8.x ``ObjectApiResponse`` (which exposes ``.body``)
    and a raw dict produced by test doubles.
    """

    body = getattr(resp, "body", None)
    if isinstance(body, dict):
        return body
    if isinstance(resp, dict):
        return resp
    return {}


def _coerce_datetime(value: Any) -> datetime:
    """Coerce an ES ``date`` value into an aware :class:`datetime`."""

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        iso = value.replace("Z", "+00:00") if value.endswith("Z") else value
        try:
            dt = datetime.fromisoformat(iso)
        except ValueError:
            return datetime.fromtimestamp(0, tz=timezone.utc)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        # ES sometimes emits millis-since-epoch.
        return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
    return datetime.fromtimestamp(0, tz=timezone.utc)


def _coerce_optional_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    return _coerce_datetime(value)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    return [str(value)]


def _encode_cve_cursor(published: datetime, cve_id: str) -> str:
    return encode_cursor("published", published.isoformat(), cve_id)


def _decode_cve_cursor(cursor: str) -> tuple[datetime, str]:
    _, iso, cve_id = decode_cursor(cursor)
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt, cve_id


def _encode_exploit_cursor(published: datetime, exploit_id: str) -> str:
    return encode_cursor("published_date", published.isoformat(), exploit_id)


def _decode_exploit_cursor(cursor: str) -> tuple[datetime, str]:
    _, iso, exploit_id = decode_cursor(cursor)
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt, exploit_id


# ---------------------------------------------------------------------------
# CVERepository
# ---------------------------------------------------------------------------


class CVERepository:
    """Read-side queries against the ``hydra-cves`` ES index and PG tables."""

    @staticmethod
    async def get_cve_detail(
        es_client: Any, cve_id: str
    ) -> CVEDetailResponse | None:
        """Return the joined NVD + EPSS + KEV view for a CVE (R11.1).

        The ``hydra-cves`` index stores one doc per ``(cve_id, source)``
        pair (see the Tier 29 adapters). The join here is a single
        ``size=50`` query filtered by ``cve_id``; we walk the results,
        pick the freshest per source, and merge. 50 docs per CVE is a
        conservative upper bound — in practice each CVE has at most one
        NVD doc, one KEV doc, and one EPSS doc per day.
        """

        query: dict[str, Any] = {
            "bool": {
                "filter": [{"term": {"cve_id": cve_id}}],
            },
        }
        try:
            raw = await es_client.search(
                index=HYDRA_CVES_INDEX,
                query=query,
                size=50,
                sort=[{"last_modified": {"order": "desc"}}],
            )
        except Exception as exc:  # noqa: BLE001 — network/ES errors
            logger.warning(
                "eas.cves.get_detail_failed",
                extra={"cve_id": cve_id, "error": str(exc)},
            )
            return None

        body = _unwrap_es_response(raw)
        hits = body.get("hits", {}).get("hits", [])
        if not hits:
            return None

        # Bucket docs by source. Each source keeps only the freshest doc
        # (the ES sort on ``last_modified DESC`` means the first doc
        # wins). ``epss`` has a per-day rhythm so we pick the latest
        # ``score_date`` / ``last_modified``.
        by_source: dict[str, dict[str, Any]] = {}
        for hit in hits:
            source_doc = hit.get("_source") if isinstance(hit, dict) else None
            if not isinstance(source_doc, dict):
                continue
            src = str(source_doc.get("source") or "nvd")
            if src not in by_source:
                by_source[src] = source_doc

        nvd = by_source.get("nvd")
        if nvd is None:
            # Without an NVD doc we don't have the base metadata
            # (published, description, CPEs). R11.1 asks for the "latest
            # nvd-cve record" specifically, so a pure-EPSS or pure-KEV
            # hit maps to a 404.
            return None

        epss = by_source.get("epss") or {}
        kev = by_source.get("kev") or {}

        return CVEDetailResponse(
            cve_id=str(nvd.get("cve_id") or cve_id),
            published=_coerce_datetime(nvd.get("published")),
            last_modified=_coerce_datetime(nvd.get("last_modified")),
            cvss_v3_score=_as_float(nvd.get("cvss_v3_score")),
            cvss_v3_vector=nvd.get("cvss_v3_vector"),
            cwe_ids=_as_list(nvd.get("cwe_ids")),
            references=_as_list(nvd.get("references")),
            affected_cpes=_as_list(nvd.get("affected_cpes")),
            description=str(nvd.get("description") or ""),
            epss_score=_as_float(epss.get("epss_score")),
            epss_percentile=_as_float(epss.get("epss_percentile")),
            kev_listed=bool(kev),
            kev_due_date=_coerce_optional_datetime(kev.get("kev_due_date") or kev.get("due_date")),
            known_ransomware_use=bool(kev.get("known_ransomware_use") or False),
        )

    @staticmethod
    async def search_cves(
        es_client: Any,
        params: CVESearchParams,
        cursor: str | None,
        limit: int,
    ) -> tuple[list[CVESearchResult], str | None]:
        """Paged CVE search (R11.3). Cursor on ``(published DESC, cve_id)``."""

        filters: list[dict[str, Any]] = [{"term": {"source": "nvd"}}]
        if params.vendor:
            filters.append({"term": {"cpe_vendor": params.vendor.lower()}})
        if params.product:
            filters.append({"term": {"cpe_product": params.product.lower()}})
        if params.min_cvss is not None:
            filters.append({"range": {"cvss_v3_score": {"gte": params.min_cvss}}})
        if params.kev_only:
            filters.append({"term": {"kev_listed": True}})
        if params.published_after:
            filters.append({"range": {"published": {"gte": params.published_after.isoformat()}}})
        if params.published_before:
            filters.append({"range": {"published": {"lte": params.published_before.isoformat()}}})

        # Cursor translates into an additional range bound. We encode
        # ``(published, cve_id)`` so that ``tie_break`` on identical
        # ``published`` values remains stable across pages.
        search_after: list[Any] | None = None
        if cursor is not None:
            cursor_dt, cursor_id = _decode_cve_cursor(cursor)
            search_after = [cursor_dt.isoformat(), cursor_id]

        query: dict[str, Any] = {"bool": {"filter": filters}}

        sort: list[dict[str, Any]] = [
            {"published": {"order": "desc"}},
            {"cve_id": {"order": "desc"}},
        ]

        kwargs: dict[str, Any] = {
            "index": HYDRA_CVES_INDEX,
            "query": query,
            "size": int(limit) + 1,
            "sort": sort,
        }
        if search_after is not None:
            kwargs["search_after"] = search_after

        try:
            raw = await es_client.search(**kwargs)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.cves.search_failed",
                extra={"error": str(exc)},
            )
            return [], None

        body = _unwrap_es_response(raw)
        hits = body.get("hits", {}).get("hits", [])

        results: list[CVESearchResult] = []
        for hit in hits:
            source_doc = hit.get("_source") if isinstance(hit, dict) else None
            if not isinstance(source_doc, dict):
                continue
            published = _coerce_datetime(source_doc.get("published"))
            results.append(
                CVESearchResult(
                    cve_id=str(source_doc.get("cve_id") or hit.get("_id") or ""),
                    cvss_v3_score=_as_float(source_doc.get("cvss_v3_score")),
                    epss_score=_as_float(source_doc.get("epss_score")),
                    kev_listed=bool(source_doc.get("kev_listed") or False),
                    published=published,
                )
            )

        next_cursor: str | None = None
        if len(results) > limit:
            results = results[:limit]
            last = results[-1]
            next_cursor = _encode_cve_cursor(last.published, last.cve_id)

        return results, next_cursor

    @staticmethod
    async def search_exploits(
        es_client: Any,
        params: ExploitSearchParams,
        cursor: str | None,
        limit: int,
    ) -> tuple[list[ExploitSearchResult], str | None]:
        """Paged exploit search — ExploitDB + Metasploit fan-in (R11.5)."""

        filters: list[dict[str, Any]] = [
            {"terms": {"source": ["exploitdb", "metasploit"]}},
        ]
        if params.cve_id:
            filters.append({"term": {"cve_ids": params.cve_id}})
        if params.platform:
            filters.append({"term": {"platforms": params.platform}})
        if params.type:
            filters.append({"term": {"type": params.type}})
        if params.published_after:
            filters.append(
                {"range": {"published_date": {"gte": params.published_after.isoformat()}}}
            )

        search_after: list[Any] | None = None
        if cursor is not None:
            cursor_dt, cursor_id = _decode_exploit_cursor(cursor)
            search_after = [cursor_dt.isoformat(), cursor_id]

        query: dict[str, Any] = {"bool": {"filter": filters}}
        sort: list[dict[str, Any]] = [
            {"published_date": {"order": "desc"}},
            {"exploit_id": {"order": "desc"}},
        ]
        kwargs: dict[str, Any] = {
            "index": HYDRA_CVES_INDEX,
            "query": query,
            "size": int(limit) + 1,
            "sort": sort,
        }
        if search_after is not None:
            kwargs["search_after"] = search_after

        try:
            raw = await es_client.search(**kwargs)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.exploits.search_failed",
                extra={"error": str(exc)},
            )
            return [], None

        body = _unwrap_es_response(raw)
        hits = body.get("hits", {}).get("hits", [])

        results: list[ExploitSearchResult] = []
        for hit in hits:
            source_doc = hit.get("_source") if isinstance(hit, dict) else None
            if not isinstance(source_doc, dict):
                continue
            exploit_id = str(
                source_doc.get("exploit_id")
                or source_doc.get("module_path")
                or hit.get("_id")
                or ""
            )
            if not exploit_id:
                continue
            # ``platform`` is stored as ``platforms`` (a list) — surface
            # the first one for the response.
            platforms = source_doc.get("platforms") or source_doc.get("platform")
            platform_label = None
            if isinstance(platforms, list) and platforms:
                platform_label = str(platforms[0])
            elif isinstance(platforms, str):
                platform_label = platforms
            results.append(
                ExploitSearchResult(
                    source=str(source_doc.get("source") or ""),
                    exploit_id=exploit_id,
                    title=str(source_doc.get("title") or source_doc.get("description") or ""),
                    type=source_doc.get("type") or source_doc.get("module_type"),
                    platform=platform_label,
                    published_date=_coerce_optional_datetime(
                        source_doc.get("published_date")
                        or source_doc.get("disclosure_date")
                    ),
                    cve_ids=_as_list(source_doc.get("cve_ids")),
                    source_url=source_doc.get("source_url"),
                )
            )

        next_cursor: str | None = None
        if len(results) > limit:
            results = results[:limit]
            last = results[-1]
            pub = last.published_date or datetime.fromtimestamp(0, tz=timezone.utc)
            next_cursor = _encode_exploit_cursor(pub, last.exploit_id)

        return results, next_cursor

    @staticmethod
    async def list_affected_assets(
        pg_pool: Any,
        cve_id: str,
        tenant_id: UUID,
    ) -> list[Asset]:
        """Return tenant-owned assets affected by ``cve_id`` (R11.4).

        Joins ``correlation_results cr`` (pipeline_id='cve_correlation')
        against ``asset_exposures ae`` via ``cr.record_b_hash =
        ae.record_hash``, filters by ``ae.tenant_id = tenant_id``, and
        joins onto the ``normalized_records`` row for the CVE side to
        match the CVE id in its payload. Distinct by ``asset_id`` so
        multiple correlations for the same asset collapse to one row.
        """

        sql = """
            SELECT DISTINCT
                a.asset_id, a.tenant_id, a.asset_type, a.normalized_value,
                a.raw_value, a.is_active, a.capture_screenshots, a.notes,
                a.created_at, a.deactivated_at
            FROM correlation_results cr
            JOIN asset_exposures ae ON ae.record_hash = cr.record_b_hash
            JOIN assets a ON a.asset_id = ae.asset_id
            JOIN normalized_records nr ON nr.raw_hash = cr.record_a_hash
            WHERE cr.pipeline_id = 'cve_correlation'
              AND ae.tenant_id = $1
              AND a.tenant_id = $1
              AND nr.payload->>'cve_id' = $2
            ORDER BY a.created_at DESC, a.asset_id
        """
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(sql, tenant_id, cve_id)

        assets: list[Asset] = []
        for row in rows:
            # Use row["notes"] tolerantly because some doubles omit it.
            try:
                notes = row["notes"]
            except (KeyError, IndexError):
                notes = None
            assets.append(
                Asset(
                    asset_id=row["asset_id"],
                    tenant_id=row["tenant_id"],
                    asset_type=row["asset_type"],
                    normalized_value=row["normalized_value"],
                    raw_value=row["raw_value"],
                    is_active=row["is_active"],
                    capture_screenshots=row["capture_screenshots"],
                    created_at=row["created_at"],
                    deactivated_at=row["deactivated_at"],
                    notes=notes,
                )
            )
        return assets
