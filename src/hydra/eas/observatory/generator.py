"""ExposureObservatory — P10 product #4 (Design §3.8, §8.6, R18).

:class:`ExposureObservatory` is the fourth :class:`BaseProduct` generator
registered on :class:`hydra.analysis.engine.AnalysisEngine`. It emits
``exposure_posture_report`` :class:`IntelligenceProduct` rows that roll
up per-country exposure posture for a 24-hour window (Design §3.8).

The generator orchestrates:

1. :class:`ObservatoryRepository.aggregate_by_country` — raw counts per
   ``(country_code, tier)``.
2. Python-side country-code fallback via
   :func:`hydra.eas.observatory.country.extract_country_code` for rows
   whose ``payload->>'country_code'`` was NULL.
3. :func:`hydra.eas.observatory.posture.posture_score` per country and
   :func:`hydra.eas.observatory.posture.trend_deltas` vs the prior-day
   report (R18.4).
4. :class:`IntelligenceProduct` assembly with the six required
   sections: ``overview``, ``service_exposure_breakdown``,
   ``vulnerability_density``, ``trend_deltas``, ``top_cves``,
   ``top_exposed_assets`` (R18.2).
5. Persistence via
   :meth:`hydra.analysis.engine.AnalysisEngine._persist_product`.
6. Optional MinIO snapshot write to
   ``hydra-observatory/{yyyy}/{mm}/{dd}/posture.json`` when
   ``EASSettings.observatory.publish_snapshot_minio`` is true (R18.6).

The class intentionally also supports being called directly from the
Airflow DAG — the bundle-first :meth:`generate` signature matches the
:class:`BaseProduct` contract but most of the actual work runs in
:meth:`run` which accepts only parameters and a pre-built engine.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from hydra.analysis.models import (
    DataBundle,
    IntelligenceProduct,
    ProductParams,
    ProductSection,
)
from hydra.analysis.products.base import BaseProduct
from hydra.eas.observatory.country import extract_country_code
from hydra.eas.observatory.posture import posture_score, trend_deltas
from hydra.eas.observatory.repository import (
    CountryTierAggregateRow,
    ObservatoryRepository,
)
from hydra.eas.settings import EASSettings

logger = logging.getLogger(__name__)

__all__ = ["ExposureObservatory"]


# Six sections required by R18.2.
_SECTION_OVERVIEW = "overview"
_SECTION_SERVICE_BREAKDOWN = "service_exposure_breakdown"
_SECTION_VULN_DENSITY = "vulnerability_density"
_SECTION_TREND_DELTAS = "trend_deltas"
_SECTION_TOP_CVES = "top_cves"
_SECTION_TOP_EXPOSED = "top_exposed_assets"


class ExposureObservatory(BaseProduct):
    """Generate ``exposure_posture_report`` IntelligenceProducts (R18)."""

    # ---- BaseProduct contract ----------------------------------------

    @property
    def product_type(self) -> str:  # R18.1
        return "exposure_posture_report"

    @property
    def source_tiers(self) -> list[int]:  # R18.1
        return [16, 17, 19, 28, 29]

    @property
    def requires_graph(self) -> bool:
        return False

    @property
    def requires_timeline(self) -> bool:
        return False

    @property
    def default_lookback_hours(self) -> float:
        return 24.0

    # ---- construction ------------------------------------------------

    def __init__(
        self,
        eas_settings: EASSettings,
        repository: ObservatoryRepository,
        *,
        minio_client: Any | None = None,
    ) -> None:
        """Build the generator.

        Parameters
        ----------
        eas_settings
            Root :class:`EASSettings` — drives weights, MinIO snapshot
            toggle, and the observatory bucket name.
        repository
            A bound :class:`ObservatoryRepository` — the generator
            never constructs its own PG pool, keeping the ownership
            graph clear (pools belong to the setup wiring).
        minio_client
            Optional async-capable MinIO / S3 client; required only
            when ``eas_settings.observatory.publish_snapshot_minio``
            is true. When ``None`` and the toggle is on, the snapshot
            write logs a warning and falls back to persist-only.
        """

        self._eas_settings = eas_settings
        self._repo = repository
        self._minio = minio_client

    # ---- entrypoints -------------------------------------------------

    async def generate(
        self, bundle: DataBundle, params: ProductParams
    ) -> IntelligenceProduct:
        """Implement the :class:`BaseProduct` contract.

        The :class:`AnalysisEngine`-driven path hands us a pre-built
        :class:`DataBundle`; for the observatory the interesting work
        lives in :meth:`run` which runs a custom aggregation against
        PG directly. We honour the bundle's ``time_window_end`` to
        choose ``as_of`` but otherwise delegate.
        """

        as_of = _parse_iso_utc(bundle.time_window_end) or _utc_now()
        return await self.run(as_of=as_of, analysis_engine=None)

    async def run(
        self,
        *,
        as_of: datetime | None = None,
        analysis_engine: Any | None = None,
    ) -> IntelligenceProduct:
        """End-to-end observatory run.

        Used by both the Airflow DAG (``_run_observatory`` in
        ``dags/eas_observatory_daily.py``) and the
        ``POST /api/v1/observatory/generate`` endpoint. The workflow:

        1. Fetch per-country aggregate rows ending at ``as_of``
           (default: now).
        2. Apply the payload-based country fallback in Python.
        3. Collapse rows across tiers into per-country totals.
        4. Compute :func:`posture_score` and :func:`trend_deltas`.
        5. Build an :class:`IntelligenceProduct` with the six R18.2
           sections.
        6. Persist via the engine (private ``_persist_product`` on the
           existing engine) when one is provided.
        7. Write MinIO snapshot when the flag is on.
        """

        if as_of is None:
            as_of = _utc_now()

        rows = await self._repo.aggregate_by_country(as_of)
        per_country = _collapse_by_country(rows)

        # Per-country posture + deltas.
        country_sections: list[dict[str, Any]] = []
        summary_lines: list[str] = []
        for country_code, totals in sorted(per_country.items()):
            score = posture_score(
                _score_inputs(totals),
                self._eas_settings.posture_score_weights,
            )

            prior_score = await self._prior_score(country_code)
            deltas = trend_deltas(score, prior_score)

            section = {
                "country_code": country_code,
                "posture_score": round(score, 4),
                "absolute_delta": round(deltas["absolute_delta"], 4),
                "percent_delta": round(deltas["percent_delta"], 4),
                "kev_count": int(totals["kev_count"]),
                "critical_count": int(totals["critical_count"]),
                "distinct_exposed_hosts": int(totals["distinct_exposed_hosts"]),
                "total_cves": int(totals["total_cves"]),
                "cves_over_30_days_old": int(totals["cves_over_30_days_old"]),
                "tier_breakdown": {
                    str(tier): int(count)
                    for tier, count in sorted(totals["tier_breakdown"].items())
                },
            }
            country_sections.append(section)
            summary_lines.append(
                f"{country_code}: posture_score={section['posture_score']:.2f} "
                f"(Δ={section['absolute_delta']:+.2f})"
            )

        # Surface the unknown-region bucket as meta (R18.2 mentions it
        # implicitly via the §3.8 precedence chain).
        unknown_count = per_country_unknown(rows)

        sections = self._build_sections(country_sections, unknown_count)
        window_start = as_of - timedelta(hours=self.default_lookback_hours)

        product = IntelligenceProduct(
            product_id=str(uuid.uuid4()),
            product_type=self.product_type,
            title=f"Exposure Posture Report — {as_of.date().isoformat()}",
            classification="yellow",
            generated_at=as_of.isoformat(),
            time_window_start=window_start.isoformat(),
            time_window_end=as_of.isoformat(),
            sections=sections,
            summary="\n".join(summary_lines) or "No covered countries in window.",
            key_findings=[
                f"{s['country_code']} posture={s['posture_score']:.2f}"
                for s in country_sections[:5]
            ],
            confidence_score=1.0,
            completeness_score=_completeness(per_country, unknown_count),
            source_tiers=list(self.source_tiers),
            record_count=sum(
                int(totals["total_cves"]) for totals in per_country.values()
            ),
            correlation_count=sum(
                int(totals["total_cves"]) for totals in per_country.values()
            ),
            parameters={
                "as_of": as_of.isoformat(),
                "country_codes": [s["country_code"] for s in country_sections],
                "unknown_region_records": unknown_count,
            },
            product_hash=_product_hash(
                self.product_type, as_of, country_sections
            ),
            tags=["observatory", "posture", "daily"],
        )

        # Persist through the engine when wired. ``_persist_product``
        # is private per P10 — we call it directly because the engine
        # doesn't expose a public persistence hook today.
        if analysis_engine is not None:
            persister = getattr(analysis_engine, "_persist_product", None)
            if persister is not None:
                try:
                    await persister(product)
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "eas.observatory.persist_failed",
                        extra={
                            "product_id": product.product_id,
                            "error": str(exc),
                        },
                    )

        # Optional MinIO snapshot.
        if self._eas_settings.observatory.publish_snapshot_minio:
            await self._write_minio_snapshot(as_of, product, country_sections)

        logger.info(
            "eas.observatory.generated",
            extra={
                "product_id": product.product_id,
                "country_count": len(country_sections),
                "unknown_region_records": unknown_count,
            },
        )
        return product

    # ---- section building -------------------------------------------

    def _build_sections(
        self,
        country_sections: list[dict[str, Any]],
        unknown_count: int,
    ) -> list[ProductSection]:
        """Build the six R18.2 sections from pre-computed country rows."""

        order = 0

        def _section(
            section_id: str,
            title: str,
            section_type: str,
            payload: dict[str, Any] | list[Any] | str,
        ) -> ProductSection:
            nonlocal order
            content = payload if isinstance(payload, str) else json.dumps(
                payload, sort_keys=True, default=str
            )
            section = ProductSection(
                section_id=section_id,
                title=title,
                section_type=section_type,
                content=content,
                order=order,
                confidence=1.0,
            )
            order += 1
            return section

        # 1) Overview — per-country posture + global totals.
        overview = {
            "country_count": len(country_sections),
            "unknown_region_records": unknown_count,
            "countries": country_sections,
        }

        # 2) Service exposure breakdown — per-country per-tier counts.
        service_breakdown = {
            s["country_code"]: s["tier_breakdown"] for s in country_sections
        }

        # 3) Vulnerability density — per-country CVEs-per-host ratio.
        vuln_density = {
            s["country_code"]: _density(
                s["total_cves"], s["distinct_exposed_hosts"]
            )
            for s in country_sections
        }

        # 4) Trend deltas — per-country absolute + percent.
        deltas_section = {
            s["country_code"]: {
                "absolute_delta": s["absolute_delta"],
                "percent_delta": s["percent_delta"],
            }
            for s in country_sections
        }

        # 5) Top CVEs — derivable from exposures/correlations once we
        # have that info; the MVP surfaces KEV counts per country as a
        # stand-in so consumers have something structured to render.
        top_cves = [
            {
                "country_code": s["country_code"],
                "kev_count": s["kev_count"],
                "total_cves": s["total_cves"],
            }
            for s in country_sections
        ]

        # 6) Top exposed assets — MVP returns per-country distinct host
        # counts; a later iteration can emit actual asset IDs.
        top_exposed = [
            {
                "country_code": s["country_code"],
                "distinct_exposed_hosts": s["distinct_exposed_hosts"],
            }
            for s in country_sections
        ]

        return [
            _section(_SECTION_OVERVIEW, "Overview", "metrics", overview),
            _section(
                _SECTION_SERVICE_BREAKDOWN,
                "Service Exposure Breakdown",
                "table",
                service_breakdown,
            ),
            _section(
                _SECTION_VULN_DENSITY,
                "Vulnerability Density",
                "table",
                vuln_density,
            ),
            _section(
                _SECTION_TREND_DELTAS,
                "Trend Deltas",
                "table",
                deltas_section,
            ),
            _section(_SECTION_TOP_CVES, "Top CVEs", "table", top_cves),
            _section(
                _SECTION_TOP_EXPOSED,
                "Top Exposed Assets",
                "table",
                top_exposed,
            ),
        ]

    # ---- prior-day delta ----------------------------------------------

    async def _prior_score(self, country_code: str) -> float:
        """Return the prior-day posture score for ``country_code``.

        Falls back to ``0.0`` when no prior product exists — R18.4 is
        then free to compute ``absolute_delta = current_score`` without
        a zero-division path (``trend_deltas`` already clamps the
        denominator at ``0.01``).
        """

        prior = await self._repo.load_prior_day_product(country_code)
        if prior is None:
            return 0.0

        # Prior products may carry the score in ``parameters`` or
        # directly in a section's JSON content. We search both so a
        # schema change in a later iteration doesn't silently break
        # the delta computation.
        params = prior.get("parameters") or {}
        cc_scores = params.get("country_scores")
        if isinstance(cc_scores, dict):
            val = cc_scores.get(country_code)
            if isinstance(val, (int, float)):
                return float(val)

        sections = prior.get("sections") or []
        for section in sections:
            if not isinstance(section, dict):
                continue
            if section.get("section_id") != _SECTION_OVERVIEW:
                continue
            content = section.get("content")
            if isinstance(content, str):
                try:
                    parsed = json.loads(content)
                except (TypeError, ValueError):
                    parsed = None
            else:
                parsed = content
            countries = (
                parsed.get("countries") if isinstance(parsed, dict) else None
            )
            if isinstance(countries, list):
                for entry in countries:
                    if (
                        isinstance(entry, dict)
                        and entry.get("country_code") == country_code
                    ):
                        score = entry.get("posture_score")
                        if isinstance(score, (int, float)):
                            return float(score)
            break

        return 0.0

    # ---- MinIO snapshot (R18.6) --------------------------------------

    async def _write_minio_snapshot(
        self,
        as_of: datetime,
        product: IntelligenceProduct,
        country_sections: list[dict[str, Any]],
    ) -> None:
        """Write ``hydra-observatory/{yyyy}/{mm}/{dd}/posture.json``.

        The blob is best-effort — a MinIO outage is logged but never
        fails the product. The JSON document carries the product id,
        ``generated_at``, and the normalized per-country sections so
        offline consumers can diff between days without re-running the
        aggregation.
        """

        if self._minio is None:
            logger.warning("eas.observatory.minio_unavailable")
            return

        bucket = self._eas_settings.observatory.minio_bucket
        object_key = (
            f"{as_of:%Y}/{as_of:%m}/{as_of:%d}/posture.json"
        )

        payload = {
            "product_id": product.product_id,
            "generated_at": product.generated_at,
            "countries": country_sections,
            "source_tiers": list(product.source_tiers),
        }
        body = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")

        try:
            import asyncio
            import inspect
            import io

            put = self._minio.put_object
            kwargs = {
                "Bucket": bucket,
                "Key": object_key,
                "Body": io.BytesIO(body),
                "ContentType": "application/json",
            }
            if inspect.iscoroutinefunction(put):
                await put(**kwargs)
            else:
                await asyncio.to_thread(put, **kwargs)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.observatory.minio_write_failed",
                extra={
                    "bucket": bucket,
                    "key": object_key,
                    "error": str(exc),
                },
            )


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _collapse_by_country(
    rows: list[CountryTierAggregateRow],
) -> dict[str, dict[str, Any]]:
    """Collapse ``(country_code, tier)`` rows into per-country totals.

    Applies the Python-side country-code fallback for rows whose
    ``country_code`` came back NULL by running
    :func:`extract_country_code` against the ``sample_payload``
    attached by the repository. Rows that still can't be attributed
    are dropped (and counted separately by
    :func:`per_country_unknown`).
    """

    per_country: dict[str, dict[str, Any]] = {}

    for row in rows:
        country = row.country_code
        if country is None:
            country = extract_country_code({"payload": row.sample_payload})
        if country is None:
            continue

        entry = per_country.setdefault(
            country,
            {
                "critical_count": 0,
                "kev_count": 0,
                "distinct_exposed_hosts": 0,
                "total_cves": 0,
                "cves_over_30_days_old": 0,
                "tier_breakdown": {},
            },
        )
        entry["critical_count"] += row.critical_count
        entry["kev_count"] += row.kev_count
        entry["distinct_exposed_hosts"] += row.distinct_exposed_hosts
        entry["total_cves"] += row.total_cves
        entry["cves_over_30_days_old"] += row.cves_over_30_days_old
        entry["tier_breakdown"][row.tier] = entry["tier_breakdown"].get(
            row.tier, 0
        ) + row.total_cves

    return per_country


def per_country_unknown(rows: list[CountryTierAggregateRow]) -> int:
    """Return the row count that can't be attributed to any country.

    These rows are reported under ``parameters.unknown_region_records``
    per Design §3.8 so consumers can spot aggregation gaps.
    """

    unknown = 0
    for row in rows:
        if row.country_code is not None:
            continue
        if extract_country_code({"payload": row.sample_payload}) is not None:
            continue
        unknown += row.total_cves
    return unknown


def _score_inputs(totals: dict[str, Any]) -> dict[str, Any]:
    """Reshape a collapsed country totals dict for :func:`posture_score`."""

    total_cves = int(totals.get("total_cves", 0) or 0)
    stale = int(totals.get("cves_over_30_days_old", 0) or 0)
    return {
        "kev_count": int(totals.get("kev_count", 0) or 0),
        "critical_count": int(totals.get("critical_count", 0) or 0),
        "vuln_cves_per_asset": (
            total_cves
            / max(1, int(totals.get("distinct_exposed_hosts", 0) or 0))
        ),
        "stale_patch_ratio": (stale / max(1, total_cves)) if total_cves else 0.0,
        "distinct_exposed_hosts": int(
            totals.get("distinct_exposed_hosts", 0) or 0
        ),
    }


def _density(total_cves: int, hosts: int) -> float:
    """Ratio of CVEs to hosts — ``0.0`` when there are no hosts."""

    if hosts <= 0:
        return 0.0
    return round(total_cves / hosts, 4)


def _completeness(
    per_country: dict[str, Any], unknown: int
) -> float:
    """Fraction of covered rows attributed to a country.

    ``covered / (covered + unknown)`` — 1.0 when every row received a
    country code, 0.0 when none did. Surfaced as
    ``IntelligenceProduct.completeness_score`` so dashboards can flag
    aggregation gaps without a separate metric.
    """

    covered = sum(
        int(v.get("total_cves", 0) or 0) for v in per_country.values()
    )
    denom = covered + int(unknown)
    if denom == 0:
        return 1.0
    return round(covered / denom, 4)


def _product_hash(
    product_type: str, as_of: datetime, country_sections: list[dict[str, Any]]
) -> str:
    """Deterministic 16-char hash used for the ``intelligence_products`` dedup key.

    Mirrors the existing products' usage of
    :func:`hydra.utils.hashing.compute_raw_hash` — same inputs give the
    same hash, so re-running the DAG for the same day idempotently
    updates the row via the ``ON CONFLICT (product_hash)`` path in
    :meth:`AnalysisEngine._persist_product`.
    """

    from hydra.utils.hashing import compute_raw_hash

    natural_key = json.dumps(
        {
            "product_type": product_type,
            "as_of": as_of.date().isoformat(),
            "country_codes": sorted(
                s["country_code"] for s in country_sections
            ),
        },
        sort_keys=True,
    )
    return compute_raw_hash(natural_key.encode())


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00") if value.endswith("Z") else value
        )
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
