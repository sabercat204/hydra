"""CVE correlation pipeline (P9 pipeline #4, R10, Design §3.4, §8.3).

Subclasses :class:`hydra.correlation.pipelines.base.BasePipeline`:

* ``pipeline_id = "cve_correlation"``
* ``source_tiers = [16, 17, 28, 29]`` — Tier 29 for CVE records,
  Tiers 16/17/28 for fingerprint records.

``correlate`` walks the candidates twice:

1. Split the records into "CVE records" (Tier 29 / source=``nvd``) and
   "fingerprint records" (everything else in the candidate set).
2. For every `(CVE, fingerprint)` pair, call :func:`cpe_matches` on
   each CPE in ``CVE.payload["affected_cpes"]`` against the
   :class:`FingerprintTriple` extracted from the fingerprint record.
3. On a positive match, build a :class:`CorrelationResult` with:

   * natural key ``(pipeline_id, record_a_hash, record_b_hash)`` =
     ``("cve_correlation", C.raw_hash, R.raw_hash)``,
   * ``confidence = min(1.0, 0.5 + 0.1 * cvss_v3_score)``,
   * evidence ``{cpe_match: [products...], cvss_v3_score, epss_score?,
     kev_listed?}``,
   * and a deterministic ``correlation_hash`` computed via
     :func:`hydra.utils.hashing.compute_raw_hash` from the same natural
     key — mirroring the scheme used by the other P9 pipelines so that
     :class:`CorrelationEngine._deduplicate` can ``ON CONFLICT`` against
     prior runs.

The final list is sorted per Design §3.4:

    (cvss_v3_score DESC, kev_listed DESC, epss_score DESC, C.raw_hash ASC)

Ties on the first three columns fall through to ``record_a_hash ASC``
(i.e. the CVE hash) so that the output list is **totally ordered**.
Combined with the natural-key dedup at the end, two invocations on the
same input produce byte-identical :class:`CorrelationResult` lists
(Property 17 / R10.5 / R27.7).

The pipeline accepts an optional :class:`AssetMonitor`. When supplied,
the *shape* of the exposure-emit path is preserved — but the current
task wiring stops short of querying tenant assets from inside the
pipeline because the pipeline does not own a PG pool. The TODO in
:meth:`_maybe_emit_exposure` points at task 10.4 (geospatial
repository) and the subsequent wiring in ``setup_eas`` that will plumb
the PG pool through.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from hydra.correlation.models import CandidateSet, CorrelationResult
from hydra.correlation.pipelines.base import BasePipeline
from hydra.eas.cves.cpe_matcher import FingerprintTriple, cpe_matches, parse_cpe
from hydra.eas.cves.fingerprint import FingerprintExtractor
from hydra.eas.settings import EASSettings
from hydra.models.normalized import NormalizedRecord
from hydra.utils.hashing import compute_raw_hash

logger = logging.getLogger(__name__)

__all__ = ["CVEPipeline"]


class CVEPipeline(BasePipeline):
    """CVE-to-fingerprint correlation pipeline (R10, Design §8.3)."""

    #: ``pipeline_id`` is read by :class:`CorrelationEngine.register_pipeline`
    #: and is also used as the natural-key prefix when deduplicating
    #: results in PG.
    pipeline_id = "cve_correlation"  # type: ignore[assignment]

    #: The tiers the correlation engine queries when building the
    #: candidate set. Tier 29 supplies the CVE records; tiers 16 / 17 /
    #: 28 supply fingerprint-bearing records (Design §3.4, R10.1).
    source_tiers = [16, 17, 28, 29]  # type: ignore[assignment]

    # Keep the confidence threshold at the base-class default (0.5). The
    # minimum produced by the R10.3 formula is `0.5 + 0.1 * 0.0 = 0.5`,
    # which is exactly equal to the threshold — so every positive CPE
    # match is emitted even when ``cvss_v3_score`` is missing.

    def __init__(
        self,
        settings: EASSettings,
        asset_monitor: Any = None,
    ) -> None:
        """Create the pipeline.

        Arguments
        ---------
        settings:
            Live :class:`EASSettings` — used to pick up
            ``cve_match_mode`` and the per-tier ``cve_fingerprint_map``.
        asset_monitor:
            Optional :class:`hydra.eas.assets.monitor.AssetMonitor`. When
            supplied, ``setup_eas`` passes the real monitor here so that
            CVE-to-asset matches can later be surfaced as exposure
            events (R10.4). When ``None`` (e.g. in tests that don't wire
            the monitor), the pipeline still produces correlation rows
            but skips the exposure emit step. Either way, the pipeline's
            observable output (the returned ``list[CorrelationResult]``)
            is deterministic for a given candidate set.
        """

        self._settings = settings
        self._fp_extractor = FingerprintExtractor(settings)
        self._asset_monitor = asset_monitor

    # ------------------------------------------------------------------
    # BasePipeline.correlate
    # ------------------------------------------------------------------

    async def correlate(
        self, candidates: CandidateSet
    ) -> list[CorrelationResult]:
        """Produce deterministic correlation results for ``candidates``.

        The algorithm is a straightforward nested loop that caps at
        :attr:`BasePipeline.max_pairs_per_run` to avoid runaway work on
        pathological candidate sets. Emission order is handled by the
        final sort + dedup step so the loop itself can be unordered.
        """

        cve_records, fingerprint_records = self._partition(candidates)

        if not cve_records or not fingerprint_records:
            return []

        # Key the dedup dict on the natural key so the pipeline is
        # idempotent within a single run — if the same (C, R) pair is
        # evaluated twice (unlikely but cheap to guard), only one row
        # survives.
        by_key: dict[tuple[str, str], CorrelationResult] = {}
        pairs_evaluated = 0
        cap = int(self.max_pairs_per_run)
        mode = self._settings.cve_match_mode

        for cve in cve_records:
            cpes = self._extract_cpes(cve)
            if not cpes:
                continue
            cvss = _as_float(cve.payload.get("cvss_v3_score"))
            epss = _as_float(cve.payload.get("epss_score"))
            kev_listed = bool(cve.payload.get("kev_listed") or False)

            for fp_record in fingerprint_records:
                if pairs_evaluated >= cap:
                    break
                pairs_evaluated += 1

                triple = self._fp_extractor.extract(fp_record)
                if triple is None:
                    continue

                matched_products = _match_cpes(cpes, triple, mode)
                if not matched_products:
                    continue

                result = self._build_result(
                    cve=cve,
                    fp_record=fp_record,
                    matched_products=matched_products,
                    cvss_v3_score=cvss,
                    epss_score=epss,
                    kev_listed=kev_listed,
                )
                by_key[(cve.raw_hash, fp_record.raw_hash)] = result

                # Fire-and-forget exposure emit. Failures here never
                # abort the pipeline — correlation results have already
                # been added to ``by_key`` and will flow through the
                # normal dedup / persist path.
                await self._maybe_emit_exposure(cve, fp_record, result)

            if pairs_evaluated >= cap:
                logger.warning(
                    "cve_pipeline_pair_cap_reached",
                    extra={"cap": cap, "pipeline_id": self.pipeline_id},
                )
                break

        # Deterministic sort: (cvss DESC, kev DESC, epss DESC, hash ASC).
        # ``evidence.get(...)`` reads fall back to 0 / False so rows
        # without the optional fields sort at the bottom of their group.
        ordered = sorted(
            by_key.values(),
            key=lambda r: (
                -float(r.evidence.get("cvss_v3_score") or 0.0),
                -int(bool(r.evidence.get("kev_listed"))),
                -float(r.evidence.get("epss_score") or 0.0),
                r.record_a_hash,
                r.record_b_hash,
            ),
        )
        return ordered

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _partition(
        candidates: CandidateSet,
    ) -> tuple[list[NormalizedRecord], list[NormalizedRecord]]:
        """Split the candidate set into CVE and fingerprint records.

        A record qualifies as a CVE record when:

        * ``tier == 29`` (VULNERABILITY_INTELLIGENCE), **and**
        * ``payload.source`` or ``source_meta.adapter_type`` identifies
          it as an NVD row. The Tier 29 base adapter stamps
          ``payload["source"]`` with the adapter's ``source_label``
          (``"nvd"`` for :class:`NVDCVEAdapter`), so we key off that.

        Everything else is eligible as a fingerprint record, including
        Tier 29 non-NVD sources (EPSS, KEV, ExploitDB, Metasploit) — the
        matcher will just come up empty because those records don't
        carry fingerprint strings. No filtering is required for
        correctness.
        """

        cve_records: list[NormalizedRecord] = []
        fp_records: list[NormalizedRecord] = []
        for tier, records in candidates.records.items():
            if int(tier) == 29:
                for record in records:
                    src = str(record.payload.get("source") or "").lower()
                    if src == "nvd":
                        cve_records.append(record)
                # Non-NVD tier-29 records intentionally dropped — they
                # don't have fingerprint strings and including them
                # in the fingerprint loop would just waste cycles.
                continue
            fp_records.extend(records)

        # Sort for determinism — the base-pipeline contract promises
        # deterministic output, and the candidate loader ordering isn't
        # guaranteed.
        cve_records.sort(key=lambda r: r.raw_hash)
        fp_records.sort(key=lambda r: r.raw_hash)
        return cve_records, fp_records

    @staticmethod
    def _extract_cpes(cve: NormalizedRecord) -> list[dict[str, Any]]:
        """Return the ``affected_cpes`` list parsed into structured entries.

        Each entry is stored as a dict so downstream sorting / evidence
        production can read the ``product`` straight off it. Entries
        that fail to parse are silently dropped — upstream data is
        noisy (NVD sometimes emits incomplete CPE strings).
        """

        raw_cpes = cve.payload.get("affected_cpes")
        if not isinstance(raw_cpes, list):
            return []
        parsed: list[dict[str, Any]] = []
        for raw in raw_cpes:
            if not isinstance(raw, str):
                continue
            try:
                entry = parse_cpe(raw)
            except Exception:  # noqa: BLE001 — CPE parse failure
                continue
            parsed.append(
                {
                    "entry": entry,
                    "product": entry.product,
                }
            )
        return parsed

    def _build_result(
        self,
        *,
        cve: NormalizedRecord,
        fp_record: NormalizedRecord,
        matched_products: list[str],
        cvss_v3_score: float | None,
        epss_score: float | None,
        kev_listed: bool,
    ) -> CorrelationResult:
        """Construct a :class:`CorrelationResult` for a matched pair.

        ``confidence`` follows R10.3 exactly. The ``correlation_hash``
        uses the shared ``compute_raw_hash`` helper so the dedup scheme
        is consistent with the other P9 pipelines.
        """

        cvss = float(cvss_v3_score or 0.0)
        confidence = min(1.0, 0.5 + 0.1 * cvss)

        evidence: dict[str, Any] = {
            "cpe_match": sorted(set(matched_products)),
            "cvss_v3_score": cvss,
        }
        if epss_score is not None:
            evidence["epss_score"] = float(epss_score)
        if kev_listed:
            evidence["kev_listed"] = True

        corr_hash = compute_raw_hash(
            f"{cve.raw_hash}:{fp_record.raw_hash}:{self.pipeline_id}".encode()
        )

        return CorrelationResult(
            # Deterministic UUID5 so two runs with the same natural key
            # produce the same correlation_id — aligning with Property 17.
            correlation_id=str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"hydra:cve_correlation:{cve.raw_hash}:{fp_record.raw_hash}",
                )
            ),
            pipeline_id=self.pipeline_id,
            record_a_hash=cve.raw_hash,
            record_b_hash=fp_record.raw_hash,
            tier_a=int(cve.tier),
            tier_b=int(fp_record.tier),
            confidence=confidence,
            match_dimensions={"cpe_match": 1.0},
            evidence=evidence,
            correlation_hash=corr_hash,
            # Deterministic timestamp — we can't use ``datetime.now`` if
            # Property 17 (byte-for-byte determinism) is to hold across
            # two invocations. Use the CVE record's ``last_modified``
            # when present, else its ingestion timestamp.
            created_at=_deterministic_created_at(cve, fp_record),
            tags=sorted(set(list(cve.tags) + list(fp_record.tags))),
        )

    async def _maybe_emit_exposure(
        self,
        cve: NormalizedRecord,
        fp_record: NormalizedRecord,
        result: CorrelationResult,
    ) -> None:
        """Route a CVE-triggered exposure through :class:`AssetMonitor`.

        TODO (task 10.4): query the tenant-owned asset for
        ``fp_record``'s indicator via the PG pool that will be wired
        into the pipeline by ``setup_eas``. The monitor's
        :meth:`AssetMonitor.record_exposure_from_correlation` expects a
        concrete :class:`Asset`, which the pipeline cannot currently
        produce without such a query. For the MVP we no-op here; the
        direct-ingestion path in ``AssetMonitor.on_record_ingested``
        still produces the asset exposure, so no R1-visible gap is
        introduced by the deferral.
        """

        # The body is intentionally a pass — see the TODO above. We keep
        # the hook and the parameter shape so task 10.4 can land a
        # minimal diff (turn this into the PG query + monitor call).
        del cve, fp_record, result
        return None


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _match_cpes(
    cpes: list[dict[str, Any]],
    triple: FingerprintTriple,
    mode: str,
) -> list[str]:
    """Return the product names of every CPE that matches ``triple``.

    An empty list means "no match" — the caller skips result production.
    """

    matched: list[str] = []
    for item in cpes:
        entry = item["entry"]
        try:
            if cpe_matches(entry, triple, mode):
                matched.append(str(item["product"]))
        except Exception:  # noqa: BLE001 — defensive
            continue
    return matched


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _deterministic_created_at(
    cve: NormalizedRecord, fp_record: NormalizedRecord
) -> str:
    """Return a stable ISO 8601 timestamp for the correlation.

    We prefer ``cve.payload["last_modified"]`` (present on NVD records)
    so that the timestamp is tied to the source data, not the current
    wall clock. Falls back to ``ingested_at`` on either record, then to
    the Unix epoch as a last-ditch sentinel.
    """

    candidates = [
        cve.payload.get("last_modified"),
        cve.ingested_at,
        fp_record.ingested_at,
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        if isinstance(candidate, datetime):
            dt = candidate
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        if isinstance(candidate, str) and candidate:
            return candidate
    return datetime.fromtimestamp(0, tz=timezone.utc).isoformat()
