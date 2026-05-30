"""Postgres-backed queries for the Exposure Observatory (Design §8.6, R18.2/R18.4).

:class:`ObservatoryRepository` owns two read paths:

* :meth:`aggregate_by_country` — the large GROUP-BY aggregation across
  ``normalized_records``, ``asset_exposures``, and ``correlation_results``
  whose shape is specified in Design §8.6. The repository drops the
  ``country_from_geo`` / ``country_from_payload`` SQL-side helpers
  (those DB functions don't exist in the schema) and instead returns
  per-record ``payload`` fragments so the generator layer can apply
  :func:`hydra.eas.observatory.country.extract_country_code` in Python
  for records whose ``payload->>'country_code'`` is NULL.
* :meth:`load_prior_day_product` — fetches the most recent prior-day
  ``exposure_posture_report`` covering a country (R18.4). The match
  is done on ``parameters.country_codes`` — a JSON array the
  generator stamps onto every product so a multi-country report is
  indexed for efficient retrieval.

The repository accepts any duck-typed pool exposing ``acquire()`` as an
async context manager — matching :class:`AssetRepository` / production
asyncpg pools, while staying thin enough for test doubles.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "CountryTierAggregateRow",
    "ObservatoryRepository",
]


# ---------------------------------------------------------------------------
# Row shape
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class CountryTierAggregateRow:
    """One row of the ``aggregate_by_country`` output.

    ``country_code`` may be ``None`` — the generator aggregates those
    rows under ``unknown_region_records`` (Design §3.8). The generator
    **also** applies :func:`extract_country_code` to the ``sample_payload``
    attached to the row, so a record with a ``payload.country`` but no
    ``payload.country_code`` can still be placed by the generator layer.

    All counts are plain :class:`int` — we coerce from ``Decimal`` /
    ``None`` inside :meth:`ObservatoryRepository.aggregate_by_country`
    so callers don't need to worry about PG numeric types.
    """

    country_code: str | None
    tier: int
    critical_count: int = 0
    kev_count: int = 0
    distinct_exposed_hosts: int = 0
    total_cves: int = 0
    cves_over_30_days_old: int = 0
    # A representative payload/geo for the rows rolled up into this
    # aggregate — used by the generator to recover a country code
    # when ``payload->>'country_code'`` is NULL. Empty dict when no
    # payload is available (e.g., when the generator bypasses the
    # fallback step).
    sample_payload: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


# The window-scoped aggregation from Design §8.6 with the two SQL-side
# helpers dropped (``country_from_geo`` / ``country_from_payload`` do not
# exist in the schema). The generator applies the fallback in Python on
# rows whose ``country_code`` comes back NULL by passing the
# ``sample_payload`` and ``sample_geo`` through
# :func:`extract_country_code`.
_AGGREGATE_SQL = """
WITH per_country AS (
  SELECT
    nr.payload->>'country_code'                                 AS country_code,
    nr.tier                                                     AS tier,
    COUNT(*) FILTER (WHERE ae.severity = 'critical')            AS critical_count,
    COUNT(*) FILTER (WHERE cr.evidence->>'kev_listed' = 'true') AS kev_count,
    COUNT(DISTINCT a.asset_id)                                  AS distinct_exposed_hosts,
    COUNT(DISTINCT cr.record_a_hash)                            AS total_cves,
    COUNT(DISTINCT cr.record_a_hash) FILTER (
      WHERE nr.ingested_at < now() - interval '30 days'
    )                                                           AS cves_over_30_days_old,
    MAX(nr.payload::text)                                       AS sample_payload
  FROM normalized_records nr
  LEFT JOIN asset_exposures ae     ON ae.record_hash = nr.raw_hash
  LEFT JOIN assets a               ON a.asset_id = ae.asset_id
  LEFT JOIN correlation_results cr ON cr.record_b_hash = nr.raw_hash
                                  AND cr.pipeline_id = 'cve_correlation'
  WHERE nr.ingested_at >= $1::timestamptz
    AND nr.tier IN (16, 17, 19, 28, 29)
  GROUP BY country_code, nr.tier
)
SELECT
    country_code,
    tier,
    critical_count,
    kev_count,
    distinct_exposed_hosts,
    total_cves,
    cves_over_30_days_old,
    sample_payload
FROM per_country
""".strip()


# Prior-day product lookup. ``parameters`` is a JSONB column on
# ``intelligence_products`` (see alembic revision
# ``003_create_intelligence_products``); we match ``country_codes`` as a
# JSON array element. ``jsonb ? text`` is the ``?`` operator so we use
# parameter binding without inlining user input.
_PRIOR_PRODUCT_SQL = """
SELECT product_id::text, product_type, title, classification,
       generated_at, time_window_start, time_window_end,
       sections::text, summary, key_findings,
       confidence_score, completeness_score,
       source_tiers, record_count, correlation_count,
       parameters::text, product_hash, tags, updated_at
FROM intelligence_products
WHERE product_type = 'exposure_posture_report'
  AND (parameters->'country_codes') ? $1
ORDER BY generated_at DESC
LIMIT 1
""".strip()


class ObservatoryRepository:
    """Thin wrapper over the observatory aggregation + prior-product read paths."""

    def __init__(self, pool: Any) -> None:
        """Store the async pool (``asyncpg.Pool`` shape)."""

        self._pool = pool

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    async def aggregate_by_country(
        self,
        as_of: datetime,
        *,
        window_days: int = 1,
    ) -> list[CountryTierAggregateRow]:
        """Return per-country × per-tier aggregate rows ending at ``as_of``.

        ``window_start`` is computed as ``as_of - window_days`` — the
        DAG invokes this once a day, so the default of 1 day matches
        Design §2.4 / R18.2's "last 24h" phrasing. Passing ``window_days``
        explicitly lets test fixtures (or future callers) widen the
        aggregation window without touching the SQL.

        Rows with NULL ``country_code`` are **not** filtered out at the
        SQL level — the generator needs to run the payload fallback
        (Step 3 of the country precedence chain) before a row is
        definitively labelled "unknown region". The generator is the
        owner of that discrimination, keeping this repository pure.
        """

        if self._pool is None:
            logger.warning("eas.observatory.repository.no_pool")
            return []

        window_start = as_of - timedelta(days=window_days)

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(_AGGREGATE_SQL, window_start)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "eas.observatory.repository.aggregate_failed",
                extra={"error": str(exc)},
            )
            return []

        return [_row_to_aggregate(row) for row in rows]

    # ------------------------------------------------------------------
    # Prior-day product lookup (R18.4)
    # ------------------------------------------------------------------

    async def load_prior_day_product(
        self,
        country_code: str,
    ) -> dict[str, Any] | None:
        """Return the most recent prior ``exposure_posture_report`` for a country.

        The return is a plain dict rather than an
        :class:`IntelligenceProduct` dataclass so this module stays
        import-cheap (no circular ``hydra.analysis.engine`` dep). The
        generator converts the dict into a typed product or pulls the
        fields it needs directly. ``None`` is returned when no prior
        report exists — the generator falls back to ``prior_score = 0.0``
        so first-day runs work cleanly (R18.4).
        """

        if self._pool is None:
            return None

        if not isinstance(country_code, str) or len(country_code) != 2:
            return None

        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    _PRIOR_PRODUCT_SQL, country_code.upper()
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.observatory.repository.prior_lookup_failed",
                extra={"country_code": country_code, "error": str(exc)},
            )
            return None

        if row is None:
            return None

        # Translate the row into a dict with JSON columns parsed. We
        # keep the interface uniform whether the row is a Mapping or
        # an asyncpg ``Record`` (supports item access + ``get``).
        return _row_to_dict(row)


# ---------------------------------------------------------------------------
# Row adapters
# ---------------------------------------------------------------------------


def _as_int(value: Any) -> int:
    """Coerce PG integer/decimal columns into :class:`int`."""

    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _row_to_aggregate(row: Any) -> CountryTierAggregateRow:
    """Convert a PG row into :class:`CountryTierAggregateRow`."""

    country_code = _row_get(row, "country_code")
    if isinstance(country_code, str):
        stripped = country_code.strip()
        country_code = stripped.upper() if len(stripped) == 2 else None
    else:
        country_code = None

    tier = _as_int(_row_get(row, "tier"))

    sample_payload_raw = _row_get(row, "sample_payload")
    sample_payload: dict[str, Any] = {}
    if isinstance(sample_payload_raw, str) and sample_payload_raw:
        try:
            decoded = json.loads(sample_payload_raw)
            if isinstance(decoded, dict):
                sample_payload = decoded
        except (TypeError, ValueError):
            sample_payload = {}
    elif isinstance(sample_payload_raw, dict):
        sample_payload = sample_payload_raw

    return CountryTierAggregateRow(
        country_code=country_code,
        tier=tier,
        critical_count=_as_int(_row_get(row, "critical_count")),
        kev_count=_as_int(_row_get(row, "kev_count")),
        distinct_exposed_hosts=_as_int(_row_get(row, "distinct_exposed_hosts")),
        total_cves=_as_int(_row_get(row, "total_cves")),
        cves_over_30_days_old=_as_int(_row_get(row, "cves_over_30_days_old")),
        sample_payload=sample_payload,
    )


def _row_get(row: Any, key: str) -> Any:
    """Return ``row[key]`` handling both :class:`dict` and asyncpg ``Record``."""

    if row is None:
        return None
    try:
        return row[key]
    except (KeyError, TypeError):
        if isinstance(row, dict):
            return row.get(key)
        return None


def _row_to_dict(row: Any) -> dict[str, Any]:
    """Convert a PG row into a plain dict.

    ``sections`` / ``parameters`` come back as JSON text (see SELECT
    list in :data:`_PRIOR_PRODUCT_SQL`); decode them here so the caller
    doesn't repeat the dance. Timestamps are left as :class:`datetime`
    objects — pydantic / the generator can serialize them later.
    """

    # asyncpg ``Record`` supports ``dict(...)``; a plain dict is
    # idempotent under that call.
    raw: dict[str, Any]
    try:
        raw = dict(row)
    except (TypeError, ValueError):
        raw = {}

    sections_raw = raw.get("sections")
    if isinstance(sections_raw, str):
        try:
            raw["sections"] = json.loads(sections_raw)
        except (TypeError, ValueError):
            raw["sections"] = []

    parameters_raw = raw.get("parameters")
    if isinstance(parameters_raw, str):
        try:
            raw["parameters"] = json.loads(parameters_raw)
        except (TypeError, ValueError):
            raw["parameters"] = {}

    return raw
