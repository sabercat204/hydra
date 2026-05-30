"""AssetMonitor — the central orchestration for R3 (Design §2.2, §8.1).

Invoked by :class:`hydra.storage.writer.AsyncWriter` for every record
successfully written to ``normalized_records``. Steps:

1. **Tier filter.** If ``record.tier`` is not in
   ``EASSettings.exposure_matching_tiers`` (default ``[16, 17, 28, 29]``)
   return immediately. This is the R3.1 gate and keeps the hot path
   cheap for the majority of records that can't produce exposures.

2. **Redis SETNX dedup** on key
   ``hydra:eas:exposure_processed:{raw_hash}`` with TTL
   ``EASSettings.exposure_dedup_ttl_seconds`` (R3.5). If another
   replica has already processed this raw_hash, return.

3. **Indicator extraction.** Delegates to :class:`IndicatorExtractor`.
   A record can yield multiple indicators; each is evaluated
   independently against ``list_matching``.

4. **Candidate lookup + final match.** ``AssetRepository.list_matching``
   pulls index-friendly candidates; :class:`AssetMatcher.is_match`
   makes the authoritative decision. The split lets DB queries stay
   predicate-simple while the matcher applies CIDR containment, domain
   suffix, and ASN logic in Python.

5. **Severity resolution.** Computed from the record's ``tier`` via
   ``EASSettings.exposure_severity_map`` per the simple rule:

   +-------+------------------------------------+----------+
   | tier  | rationale                          | severity |
   +=======+====================================+==========+
   | 16    | cyber threat intel default         | high     |
   +-------+------------------------------------+----------+
   | 19    | sanctions default                  | medium   |
   +-------+------------------------------------+----------+
   | 29    | vulnerability intel (KEV/exploit)  | critical |
   +-------+------------------------------------+----------+
   | else  | noise tier, fallback               | low      |
   +-------+------------------------------------+----------+

   The KEV / exploit bonuses that upgrade to ``critical`` live in the
   CVE_Pipeline path (:meth:`record_exposure_from_correlation`) — the
   direct ingestion path cannot see them because they require a join
   against NVD/KEV metadata.

6. **Exposure write.** ``ExposureRepository.insert_exposure`` does the
   ``ON CONFLICT DO NOTHING``; a ``None`` return means a dedup hit and
   we skip downstream work.

7. **Metrics + alert.** Increment
   ``hydra_eas_exposure_events_total`` then hand the exposure to
   :class:`ExposureAlerter` for severities in ``{"high", "critical"}``.

8. **PG backpressure.** Any exception during the insert is swallowed
   and the ``(asset, record, indicator, severity)`` tuple is pushed
   onto an in-process ``collections.deque(maxlen=10_000)``. Overflow
   increments ``hydra_eas_exposure_buffer_overflow_total``. The
   ingestion path stays healthy; a recovery task can drain the buffer
   later (out of scope for this module).

Determinism / purity note: all non-pure inputs (Redis SETNX, PG
queries, Alertmanager POST) are injected via constructor dependencies
so the method is testable with fakes and Property 7 (match
determinism) is upheld.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from hydra.eas.assets.alerter import ExposureAlerter
from hydra.eas.assets.extractor import Indicator, IndicatorExtractor
from hydra.eas.assets.matcher import AssetMatcher
from hydra.eas.assets.models import Asset
from hydra.eas.assets.repository import AssetRepository, ExposureRepository
from hydra.eas.metrics import (
    hydra_eas_exposure_buffer_overflow_total,
    hydra_eas_exposure_events_total,
)
from hydra.eas.settings import EASSettings
from hydra.models.normalized import NormalizedRecord

logger = logging.getLogger(__name__)

__all__ = ["AssetMonitor"]


_DEDUP_KEY_PREFIX = "hydra:eas:exposure_processed:"
_BUFFER_MAXLEN = 10_000


@dataclass(slots=True, frozen=True)
class _BufferedExposure:
    """One entry in the in-process overflow buffer."""

    asset: Asset
    record_hash: str
    tier: int
    matched_indicator: str
    severity: str
    tenant_id: UUID


class AssetMonitor:
    """Wire everything together for the R3 exposure-matching flow."""

    def __init__(
        self,
        settings: EASSettings,
        asset_repo: AssetRepository,
        exposure_repo: ExposureRepository,
        extractor: IndicatorExtractor,
        matcher: AssetMatcher,
        alerter: ExposureAlerter,
        redis_cache: Any,
    ) -> None:
        self._settings = settings
        self._asset_repo = asset_repo
        self._exposure_repo = exposure_repo
        self._extractor = extractor
        self._matcher = matcher
        self._alerter = alerter
        self._redis = redis_cache
        # Bounded deque — when full, ``append`` silently drops the
        # oldest item. We surface that drop via the
        # ``exposure_buffer_overflow_total`` counter before the append
        # so operators can see the rate rising even if the deque size
        # itself is flat at 10 000.
        self._buffer: deque[_BufferedExposure] = deque(maxlen=_BUFFER_MAXLEN)

    # ------------------------------------------------------------------
    # Hot path — registered as a post-insert hook on StorageWriter
    # ------------------------------------------------------------------

    async def on_record_ingested(self, record: NormalizedRecord) -> None:
        """Process a freshly-written :class:`NormalizedRecord`.

        Safe to call from :meth:`AsyncWriter._process_batch`. Never
        raises under normal operation — any failure path either logs
        and returns, or routes the work into the overflow buffer.
        """

        # (1) Tier filter — R3.1 gate.
        if int(record.tier) not in self._settings.exposure_matching_tiers:
            return

        # (2) Redis SETNX dedup — R3.5.
        if not await self._try_claim_raw_hash(record.raw_hash):
            return

        # (3) Indicator extraction.
        indicators = self._extractor.extract(record)
        if not indicators:
            return

        # Resolve severity once per record — tier is fixed.
        severity = self._severity_for_tier(int(record.tier))

        # (4) / (5) / (6) / (7) per indicator.
        for indicator in indicators:
            await self._process_indicator(record, indicator, severity)

    # ------------------------------------------------------------------
    # CVE-pipeline hook — R10.4 per Design §3.4
    # ------------------------------------------------------------------

    async def record_exposure_from_correlation(
        self,
        asset: Asset,
        record_r: NormalizedRecord,
        record_c: NormalizedRecord,
        severity: str,
    ) -> None:
        """Emit an exposure event from a CVE-pipeline match.

        Called by :class:`hydra.eas.cves.pipeline.CVEPipeline` (task 9.6)
        when a ``(CVE record, fingerprint record)`` pair is produced by
        the correlation engine. Reuses steps 6–7 of the normal flow —
        we bypass the indicator extraction and matcher because the CVE
        pipeline has already decided which asset is affected.

        Arguments
        ---------
        asset:
            The asset linked to the fingerprint record.
        record_r:
            The fingerprint record (Tier 16/17/28). Its ``raw_hash``
            becomes the ``asset_exposures.record_hash`` column so that
            a user viewing exposures sees the OSINT record, not the CVE.
        record_c:
            The CVE record (Tier 29). Currently only used for logging;
            downstream the CVE metadata is accessible via the
            correlation_results join.
        severity:
            Caller-supplied severity from
            ``EASSettings.cve_severity_map`` logic.
        """

        # The matched "indicator" for a CVE-derived exposure is the CVE
        # identifier from the CVE record's payload — not a network-layer
        # string. Storing it here preserves the dedup invariant and
        # gives the UI something useful to show.
        matched_indicator = record_c.payload.get("cve_id") or record_c.raw_hash
        await self._insert_and_alert(
            asset=asset,
            record_hash=record_r.raw_hash,
            tier=int(record_r.tier),
            matched_indicator=matched_indicator,
            severity=severity,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _process_indicator(
        self,
        record: NormalizedRecord,
        indicator: Indicator,
        severity: str,
    ) -> None:
        try:
            candidates = await self._asset_repo.list_matching(indicator.value)
        except Exception as exc:
            logger.warning(
                "asset_monitor_list_matching_failed",
                extra={
                    "indicator_value": indicator.value,
                    "error": str(exc),
                },
            )
            return

        for candidate in candidates:
            if not candidate.is_active:
                continue
            if not self._matcher.is_match(indicator.value, candidate):
                continue

            await self._insert_and_alert(
                asset=candidate,
                record_hash=record.raw_hash,
                tier=int(record.tier),
                matched_indicator=indicator.value,
                severity=severity,
            )

    async def _insert_and_alert(
        self,
        *,
        asset: Asset,
        record_hash: str,
        tier: int,
        matched_indicator: str,
        severity: str,
    ) -> None:
        """Shared path for both direct and CVE-derived exposures."""

        try:
            exposure_id = await self._exposure_repo.insert_exposure(
                asset_id=asset.asset_id,
                tenant_id=asset.tenant_id,
                record_hash=record_hash,
                tier=tier,
                matched_indicator=matched_indicator,
                severity=severity,
            )
        except Exception as exc:
            # PG write error — drop into the overflow buffer so the
            # ingestion path stays healthy (Design §6.1).
            self._buffer_overflow(
                _BufferedExposure(
                    asset=asset,
                    record_hash=record_hash,
                    tier=tier,
                    matched_indicator=matched_indicator,
                    severity=severity,
                    tenant_id=asset.tenant_id,
                ),
                reason=str(exc),
            )
            return

        # Duplicate — the partial unique index fired and the row was
        # already present. Metrics and alerting are suppressed per
        # R3.3 (dedup invariance).
        if exposure_id is None:
            return

        # (7) Metrics + alert.
        hydra_eas_exposure_events_total.labels(
            tenant_id=str(asset.tenant_id),
            asset_type=asset.asset_type,
            tier=str(tier),
            severity=severity,
        ).inc()

        if severity in ("critical", "high"):
            # Build the ExposureEvent dataclass expected by the alerter
            # from the values we just wrote. We avoid a round-trip
            # fetch to keep the hot path cheap; the ID + created_at
            # pair is the only thing we couldn't have computed locally,
            # and insert_exposure returned the ID for us.
            from hydra.eas.assets.models import ExposureEvent
            from datetime import datetime, timezone

            exposure = ExposureEvent(
                exposure_id=exposure_id,
                asset_id=asset.asset_id,
                tenant_id=asset.tenant_id,
                record_hash=record_hash,
                tier=tier,
                matched_indicator=matched_indicator,
                severity=severity,
                created_at=datetime.now(timezone.utc),
            )
            try:
                await self._alerter.send(exposure, asset)
            except Exception as exc:
                # Alerter is documented as non-raising, but belt-and-
                # braces: a bug there must not knock over the ingestion
                # flow.
                logger.warning(
                    "alerter_send_failed",
                    extra={
                        "exposure_id": str(exposure_id),
                        "error": str(exc),
                    },
                )

    async def _try_claim_raw_hash(self, raw_hash: str) -> bool:
        """SETNX the dedup key; ``True`` iff we own this processing turn."""

        key = f"{_DEDUP_KEY_PREFIX}{raw_hash}"
        ttl = self._settings.exposure_dedup_ttl_seconds
        try:
            # Prefer the bespoke API when the cache is the HYDRA
            # ``RedisCache`` (no SETNX exposed) — fall back to
            # ``redis.asyncio.Redis.set(..., nx=True, ex=ttl)`` which
            # returns ``True`` on claim, ``None`` on contention.
            underlying = getattr(self._redis, "_redis", None)
            if underlying is None:
                # Tests pass a direct redis-like object in ``_redis``.
                underlying = self._redis
            result = await underlying.set(key, "1", nx=True, ex=ttl)
        except Exception as exc:
            # Redis hiccup: better to skip than to double-dispatch.
            # The next record with the same hash will likely succeed
            # once Redis recovers.
            logger.warning(
                "dedup_setnx_failed",
                extra={"raw_hash": raw_hash, "error": str(exc)},
            )
            return False
        return bool(result)

    def _severity_for_tier(self, tier: int) -> str:
        """Resolve severity label from the per-tier map."""

        sev_map = self._settings.exposure_severity_map
        if tier == 16:
            return sev_map.cyber_threat_default
        if tier == 19:
            return sev_map.sanctions_default
        if tier == 29:
            # Direct ingestion of Tier 29 records without correlation
            # context — treat as critical because the fact that a
            # vuln-intel record references a tenant asset is itself
            # a high-severity signal. Correlated hits still flow
            # through ``record_exposure_from_correlation`` with a
            # potentially-higher severity.
            return "critical"
        return "low"

    def _buffer_overflow(self, entry: _BufferedExposure, *, reason: str) -> None:
        """Push into the bounded deque and emit the overflow counter."""

        # Deque's ``append`` with a ``maxlen`` drops the oldest entry
        # silently. We count every drop — even the one that's about
        # to happen — so the counter lines up with "events we couldn't
        # process synchronously".
        if len(self._buffer) >= _BUFFER_MAXLEN:
            hydra_eas_exposure_buffer_overflow_total.labels().inc()

        self._buffer.append(entry)
        logger.warning(
            "exposure_buffered_due_to_pg_error",
            extra={
                "asset_id": str(entry.asset.asset_id),
                "record_hash": entry.record_hash,
                "reason": reason,
                "buffer_depth": len(self._buffer),
            },
        )

    # ------------------------------------------------------------------
    # Test / introspection helpers (not part of public API but useful)
    # ------------------------------------------------------------------

    @property
    def buffer_depth(self) -> int:
        """Current depth of the PG-backpressure buffer."""

        return len(self._buffer)
