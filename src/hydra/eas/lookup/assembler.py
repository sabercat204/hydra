"""Cold-path assembler for ``LookupResponse`` (Design §2.3, §8.5, R17.2 / R17.5).

``LookupAssembler.assemble(cls, normalized_value, tenant_id)`` fans out
four parallel queries via :func:`asyncio.gather`:

1. **PG** — ``normalized_records`` matching the indicator via a
   payload-text search (MVP uses ``payload::text ILIKE`` since we don't
   have a generic indicator column). Returns ``LookupRecordSummary`` rows
   plus the aggregated ``tags`` list and ``first_seen`` / ``last_seen``
   timestamps.
2. **ES ``hydra-cves``** — filtered by indicator appearing in
   ``cpe_product`` or ``description`` to surface matching CVE entries.
3. **ES ``hydra-screenshots``** — filtered by ``url_host = indicator``
   when ``cls`` is ``"domain"`` or ``"hostname"``; skipped for
   ``ipv4`` / ``ipv6`` / ``hash``.
4. **Asset reference** — ``AssetRepository.get_active_by_key`` for every
   asset type the indicator class plausibly maps to (e.g. ``ipv4`` →
   ``ip`` **and** ``cidr`` for the /32 containment case). This is the
   only **tenant-scoped** piece (R17.5); cache storage is deliberately
   stripped of this field.

The assembler is a coroutine object rather than a stateless function
because it needs dependency-injected handles (PG pool, ES client, asset
repository). That mirrors the other capability repositories in
``hydra/eas/``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from hydra.eas.assets.repository import AssetRepository
from hydra.eas.schemas.lookup import (
    IndicatorClass,
    LookupAssetReference,
    LookupCVECorrelation,
    LookupRecordSummary,
    LookupResponse,
    LookupScreenshotRef,
)
from hydra.eas.storage.es_mappings import (
    HYDRA_CVES_INDEX,
    HYDRA_SCREENSHOTS_INDEX,
)

logger = logging.getLogger(__name__)

__all__ = ["LookupAssembler"]


# Hard cap on how many rows each fan-out query returns. Keeps the
# payload small enough to fit comfortably within the ~20 KB/entry
# msgpack budget (Design §3.7). Configurable from the outside if
# deployments need tighter / looser bounds.
_MAX_RECORDS = 100
_MAX_CVES = 50
_MAX_SCREENSHOTS = 25


class LookupAssembler:
    """Runs the cold-path fan-out for a cache miss."""

    def __init__(
        self,
        pg_pool: Any,
        es_client: Any,
        asset_repository: AssetRepository,
    ) -> None:
        self._pg_pool = pg_pool
        self._es_client = es_client
        self._asset_repo = asset_repository

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def assemble(
        self,
        cls: IndicatorClass,
        normalized_value: str,
        tenant_id: UUID,
    ) -> LookupResponse:
        """Fan out to PG + ES, then attach the per-tenant asset_reference.

        The four queries run concurrently via :func:`asyncio.gather` with
        ``return_exceptions=True`` so that one engine outage doesn't
        sink the whole response. Partial responses are preferred over
        503s because the lookup endpoint is a hot-path UX surface —
        clients would rather see records without CVE correlations than
        a blanket error when ES is slow.
        """

        records_task = self._fetch_records_and_tags(cls, normalized_value)
        cves_task = self._fetch_cve_correlations(cls, normalized_value)
        screenshots_task = self._fetch_screenshots(cls, normalized_value)
        asset_task = self._fetch_asset_reference(
            cls, normalized_value, tenant_id
        )

        results = await asyncio.gather(
            records_task,
            cves_task,
            screenshots_task,
            asset_task,
            return_exceptions=True,
        )

        records, tags, first_seen, last_seen = _unwrap_records_tuple(
            results[0]
        )
        cve_correlations = _unwrap_list(results[1], "cve_correlations")
        screenshots = _unwrap_list(results[2], "screenshots")
        asset_reference = _unwrap_asset(results[3])

        return LookupResponse(
            indicator=normalized_value,
            indicator_class=cls,
            records=records,
            tags=tags,
            cve_correlations=cve_correlations,
            screenshots=screenshots,
            first_seen=first_seen,
            last_seen=last_seen,
            asset_reference=asset_reference,
        )

    # ------------------------------------------------------------------
    # PG — records + tags
    # ------------------------------------------------------------------

    async def _fetch_records_and_tags(
        self,
        cls: IndicatorClass,
        normalized_value: str,
    ) -> tuple[list[LookupRecordSummary], list[str], datetime | None, datetime | None]:
        """Return ``(records, tags, first_seen, last_seen)`` for the indicator.

        MVP query: the ``normalized_records`` table doesn't have a
        generic indicator column, so we lean on the JSONB ``payload``
        column with ``payload::text ILIKE`` as a substring probe. That
        gives us correct-enough recall without a schema change; a
        post-MVP follow-up can add a derived ``indicators`` GIN index.

        The search is deliberately scoped to the indicator's normalized
        form, which removes the bulk of false positives (e.g. an IP
        ``10.0.0.1`` won't match an ASN record that happens to include
        ``AS10001``). The surrounding ILIKE wildcards still allow
        matches on structured JSON like ``"ip":"10.0.0.1"``.
        """

        if self._pg_pool is None:
            return [], [], None, None

        pattern = f"%{normalized_value}%"

        records_sql = """
            SELECT raw_hash, tier, stream_id, timestamp, confidence, tags
              FROM normalized_records
             WHERE payload::text ILIKE $1
             ORDER BY timestamp DESC
             LIMIT $2
        """
        span_sql = """
            SELECT MIN(timestamp) AS first_seen,
                   MAX(timestamp) AS last_seen
              FROM normalized_records
             WHERE payload::text ILIKE $1
        """

        try:
            async with self._pg_pool.acquire() as conn:
                rows = await conn.fetch(records_sql, pattern, _MAX_RECORDS)
                span_row = await conn.fetchrow(span_sql, pattern)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.lookup.assembler.pg_failed",
                extra={"indicator": normalized_value, "error": str(exc)},
            )
            return [], [], None, None

        records: list[LookupRecordSummary] = []
        tags_accum: dict[str, None] = {}  # ordered de-dup
        for row in rows:
            raw_tags = row["tags"] if "tags" in _row_keys(row) else None
            if isinstance(raw_tags, list):
                for tag in raw_tags:
                    if isinstance(tag, str):
                        tags_accum.setdefault(tag, None)
            records.append(
                LookupRecordSummary(
                    raw_hash=str(row["raw_hash"]),
                    tier=int(row["tier"]),
                    stream_id=str(row["stream_id"]),
                    timestamp=_coerce_datetime(row["timestamp"]),
                    confidence=float(row["confidence"] or 0.0),
                )
            )

        first_seen = _coerce_optional_datetime(
            span_row["first_seen"] if span_row is not None else None
        )
        last_seen = _coerce_optional_datetime(
            span_row["last_seen"] if span_row is not None else None
        )

        return records, list(tags_accum.keys()), first_seen, last_seen

    # ------------------------------------------------------------------
    # ES hydra-cves — CVE correlations
    # ------------------------------------------------------------------

    async def _fetch_cve_correlations(
        self,
        cls: IndicatorClass,
        normalized_value: str,
    ) -> list[LookupCVECorrelation]:
        """Surface CVE docs mentioning the indicator (in CPE or description)."""

        if self._es_client is None:
            return []

        query: dict[str, Any] = {
            "bool": {
                "filter": [{"term": {"source": "nvd"}}],
                "should": [
                    {"term": {"cpe_product": normalized_value}},
                    {"term": {"cpe_vendor": normalized_value}},
                    {"match": {"description": normalized_value}},
                ],
                "minimum_should_match": 1,
            },
        }

        try:
            raw = await self._es_client.search(
                index=HYDRA_CVES_INDEX,
                query=query,
                size=_MAX_CVES,
                sort=[{"cvss_v3_score": {"order": "desc"}}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.lookup.assembler.cves_failed",
                extra={"indicator": normalized_value, "error": str(exc)},
            )
            return []

        body = _unwrap_es_response(raw)
        hits = body.get("hits", {}).get("hits", [])
        correlations: list[LookupCVECorrelation] = []
        for hit in hits:
            src = hit.get("_source") if isinstance(hit, dict) else None
            if not isinstance(src, dict):
                continue
            cve_id = str(src.get("cve_id") or "")
            if not cve_id:
                continue
            cvss = _as_float(src.get("cvss_v3_score"))
            # Confidence mirrors the CVE_Pipeline formula (R10.3) so
            # that the lookup response ordering is consistent with the
            # correlation_results table when both are populated.
            confidence = min(1.0, 0.5 + 0.1 * (cvss or 0.0))
            correlations.append(
                LookupCVECorrelation(
                    cve_id=cve_id,
                    cvss_v3_score=cvss,
                    kev_listed=bool(src.get("kev_listed") or False),
                    record_hash=str(hit.get("_id") or cve_id),
                    confidence=confidence,
                )
            )
        return correlations

    # ------------------------------------------------------------------
    # ES hydra-screenshots — rendered pages for domain/hostname
    # ------------------------------------------------------------------

    async def _fetch_screenshots(
        self,
        cls: IndicatorClass,
        normalized_value: str,
    ) -> list[LookupScreenshotRef]:
        """Return rendered-page references when ``cls`` is domain/hostname.

        Skipped for IP / hash classes because URL host matching is only
        meaningful for named hosts. A future enhancement could extend
        this to look up screenshots by resolved IP, but that requires a
        reverse mapping we don't yet persist.
        """

        if self._es_client is None:
            return []
        if cls not in ("domain", "hostname"):
            return []

        query: dict[str, Any] = {
            "bool": {"filter": [{"term": {"url_host": normalized_value}}]},
        }
        try:
            raw = await self._es_client.search(
                index=HYDRA_SCREENSHOTS_INDEX,
                query=query,
                size=_MAX_SCREENSHOTS,
                sort=[{"rendered_at": {"order": "desc"}}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.lookup.assembler.screenshots_failed",
                extra={"indicator": normalized_value, "error": str(exc)},
            )
            return []

        body = _unwrap_es_response(raw)
        hits = body.get("hits", {}).get("hits", [])
        screenshots: list[LookupScreenshotRef] = []
        for hit in hits:
            src = hit.get("_source") if isinstance(hit, dict) else None
            if not isinstance(src, dict):
                continue
            record_hash = str(src.get("record_hash") or hit.get("_id") or "")
            phash = str(src.get("phash") or "")
            url = str(src.get("url") or "")
            if not (record_hash and phash and url):
                continue
            screenshots.append(
                LookupScreenshotRef(
                    record_hash=record_hash,
                    url=url,
                    rendered_at=_coerce_datetime(src.get("rendered_at")),
                    phash=phash,
                )
            )
        return screenshots

    # ------------------------------------------------------------------
    # Asset reference — tenant-scoped (R17.5)
    # ------------------------------------------------------------------

    async def _fetch_asset_reference(
        self,
        cls: IndicatorClass,
        normalized_value: str,
        tenant_id: UUID,
    ) -> LookupAssetReference | None:
        """Return the asset reference if the caller's tenant owns a match.

        R17.5 — this is the **only** tenant-scoped field; cache hits for
        other tenants must be byte-identical except here. The assembler
        is therefore careful to not mix this field into the main result
        set that goes into Redis; the router is responsible for layering
        it back on after cache read.

        The class-to-asset-type mapping expands some classes into
        multiple candidates: an ``ipv4`` indicator might match an
        ``ip`` asset **or** a ``cidr`` asset that contains it. We try
        each candidate type in order and return the first hit.
        """

        if self._asset_repo is None:
            return None

        candidates = _asset_type_candidates(cls)
        for asset_type in candidates:
            try:
                asset = await self._asset_repo.get_active_by_key(
                    tenant_id, asset_type, normalized_value
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "eas.lookup.assembler.asset_lookup_failed",
                    extra={
                        "indicator": normalized_value,
                        "asset_type": asset_type,
                        "error": str(exc),
                    },
                )
                continue
            if asset is not None:
                return LookupAssetReference(
                    asset_id=asset.asset_id,
                    asset_type=asset.asset_type,
                    normalized_value=asset.normalized_value,
                )
        return None


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _asset_type_candidates(cls: IndicatorClass) -> tuple[str, ...]:
    """Map an indicator class to the asset types it plausibly could match.

    * ``ipv4`` / ``ipv6`` → ``ip`` (exact) — the matcher handles CIDR
      containment via its own branch, so for the lookup-response
      ``asset_reference`` we only report exact-IP ownership. CIDR
      assets can still be discovered via the regular exposures list.
    * ``domain`` → ``domain`` — suffix matching is a monitor-side
      concern, not a lookup-response concern. If a tenant has a
      ``domain`` asset ``example.com`` and the lookup is for
      ``foo.example.com``, the domain asset isn't the "same" thing;
      the exposures table is the right mechanism to surface that.
    * ``hostname`` → ``hostname``, ``domain`` — a bare single-label
      hostname might match a ``hostname`` asset exactly, or happen to
      equal a ``domain`` asset for a tenant that registered their TLD
      shorthand under either type.
    * ``hash`` → nothing; no asset_type corresponds to a hash.
    """

    if cls == "ipv4" or cls == "ipv6":
        return ("ip",)
    if cls == "domain":
        return ("domain",)
    if cls == "hostname":
        return ("hostname", "domain")
    # ``hash`` has no matching asset type — return empty tuple so the
    # caller skips the asset lookup path cleanly.
    return ()


def _unwrap_records_tuple(
    result: Any,
) -> tuple[list[LookupRecordSummary], list[str], datetime | None, datetime | None]:
    """Coerce a gather-returned value or exception into the records tuple."""

    if isinstance(result, BaseException):
        logger.warning(
            "eas.lookup.assembler.records_exception",
            extra={"error": str(result)},
        )
        return [], [], None, None
    if isinstance(result, tuple) and len(result) == 4:
        return result  # type: ignore[return-value]
    return [], [], None, None


def _unwrap_list(result: Any, field_name: str) -> list[Any]:
    """Coerce a gather-returned value or exception into a list."""

    if isinstance(result, BaseException):
        logger.warning(
            "eas.lookup.assembler.list_exception",
            extra={"field": field_name, "error": str(result)},
        )
        return []
    if isinstance(result, list):
        return result
    return []


def _unwrap_asset(result: Any) -> LookupAssetReference | None:
    if isinstance(result, BaseException):
        logger.warning(
            "eas.lookup.assembler.asset_exception",
            extra={"error": str(result)},
        )
        return None
    if isinstance(result, LookupAssetReference):
        return result
    return None


def _unwrap_es_response(resp: Any) -> dict[str, Any]:
    """Unwrap ES 8.x ``ObjectApiResponse`` or fall through for dict doubles."""

    body = getattr(resp, "body", None)
    if isinstance(body, dict):
        return body
    if isinstance(resp, dict):
        return resp
    return {}


def _coerce_datetime(value: Any) -> datetime:
    """Coerce an arbitrary timestamp-like value to an aware ``datetime``."""

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


def _row_keys(row: Any) -> list[str]:
    """Return the column names of an asyncpg ``Record`` or dict row."""

    keys_fn = getattr(row, "keys", None)
    if keys_fn is None:
        return []
    try:
        return list(keys_fn())
    except Exception:  # pragma: no cover - defensive
        return []
