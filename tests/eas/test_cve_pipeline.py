"""Property tests for CVEPipeline (task 9.1).

Covers:

* **Property 17: CVE_Pipeline determinism** — fixed `(CVE records,
  fingerprint records)` inputs produce identical :class:`CorrelationResult`
  rows under the natural key `(pipeline_id, record_a_hash, record_b_hash)`
  with identical ``confidence``, ``evidence``, and field ordering. Running
  the pipeline twice on the same candidates yields byte-equal lists, and
  permuting candidate order within a tier does not change the output.
* **Property 18: CVE_Pipeline confidence formula** — for every emitted
  result, ``confidence == min(1.0, 0.5 + 0.1 * cvss_v3_score)``, with
  boundary cases at 0.0, 5.0, 10.0, and the missing-CVSS case where
  the pipeline treats ``None`` as 0.0.

**Validates: Requirements 10.2, 10.3, 10.5, 27.7**

Supporting / non-PBT tests (design-driven sanity checks):

* ``test_empty_candidates_yields_empty`` — either no CVE records or no
  fingerprint records returns an empty list.
* ``test_evidence_contains_expected_keys`` — conditional inclusion of
  ``epss_score`` and ``kev_listed`` matches
  :meth:`CVEPipeline._build_result` contract.
* ``test_sort_order_design_3_4`` — `(cvss DESC, kev DESC, epss DESC,
  record_a_hash ASC)` sort is exercised with seeded rows.

All tests construct :class:`CandidateSet` directly — the pipeline is a
pure function over its inputs, so no PG / ES / Redis are required.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings as hyp_settings, strategies as st

from hydra.correlation.models import CandidateSet, CorrelationResult
from hydra.eas.cves.pipeline import CVEPipeline
from hydra.eas.settings import EASSettings
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# A fixed, deterministic ``ingested_at`` so tests never pick up
# ``datetime.now(...)``. ``_deterministic_created_at`` inside the pipeline
# prefers ``payload["last_modified"]`` but falls back to ``ingested_at`` —
# we pin both for full determinism.
_FIXED_INGESTED_AT = datetime(2024, 1, 10, 0, 0, 0, tzinfo=timezone.utc)


def _make_cve_record(
    *,
    raw_hash: str = "a" * 16,
    cve_id: str = "CVE-2024-1234",
    cvss_v3_score: float | None = 8.5,
    epss_score: float | None = 0.42,
    kev_listed: bool = False,
    affected_cpes: list[str] | None = None,
    last_modified: str = "2024-01-15T12:00:00+00:00",
) -> NormalizedRecord:
    """Build a Tier-29 NVD CVE record with deterministic timestamps."""

    payload: dict[str, Any] = {
        "source": "nvd",
        "cve_id": cve_id,
        "last_modified": last_modified,
        "affected_cpes": affected_cpes
        if affected_cpes is not None
        else ["cpe:2.3:a:apache:httpd:2.4.52:*:*:*:*:*:*:*"],
    }
    if cvss_v3_score is not None:
        payload["cvss_v3_score"] = cvss_v3_score
    if epss_score is not None:
        payload["epss_score"] = epss_score
    if kev_listed:
        payload["kev_listed"] = True

    return NormalizedRecord(
        stream_id="nvd-cve",
        tier=Tier.VULNERABILITY_INTELLIGENCE,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        geo=None,
        payload=payload,
        source_meta=SourceMeta(
            source_name="nvd", source_url="", adapter_type="rest_json"
        ),
        raw_hash=raw_hash,
        ingested_at=_FIXED_INGESTED_AT,
        tags=["nvd"],
    )


def _make_fp_record(
    *,
    raw_hash: str = "b" * 16,
    fingerprint: str = "apache/httpd/2.4.52",
    tier: int = 16,
) -> NormalizedRecord:
    """Build a fingerprint record for tiers 16/17/28 with deterministic timestamps.

    Uses the default ``cve_fingerprint_map.tier_16`` expression
    ``["$.fingerprint"]`` so plain-vanilla :class:`EASSettings` picks up the
    payload without per-test overrides.
    """

    return NormalizedRecord(
        stream_id="cybersec-threat-feed",
        tier=Tier(tier),
        timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc),
        geo=None,
        payload={"fingerprint": fingerprint},
        source_meta=SourceMeta(
            source_name="threat-feed", source_url="", adapter_type="rest_json"
        ),
        raw_hash=raw_hash,
        ingested_at=_FIXED_INGESTED_AT,
        tags=[],
    )


def _make_candidates(
    cves: list[NormalizedRecord], fps: list[NormalizedRecord]
) -> CandidateSet:
    """Wrap records in a :class:`CandidateSet` keyed by tier.

    Groups fingerprint records by their actual tier so the pipeline's
    :meth:`_partition` sees them on the right key, and collects all
    CVE records under tier 29.
    """

    records: dict[int, list[NormalizedRecord]] = {29: list(cves)}
    for fp in fps:
        records.setdefault(int(fp.tier), []).append(fp)
    return CandidateSet(
        pipeline_id="cve_correlation",
        source_tiers=[16, 17, 28, 29],
        time_window_start="2024-01-01T00:00:00+00:00",
        time_window_end="2024-01-31T00:00:00+00:00",
        records=records,
        total_records=len(cves) + len(fps),
    )


def _make_pipeline() -> CVEPipeline:
    """Default :class:`CVEPipeline` with stock :class:`EASSettings`."""

    return CVEPipeline(settings=EASSettings())


# ---------------------------------------------------------------------------
# Property 17 — determinism
# ---------------------------------------------------------------------------


async def test_property_determinism_byte_equal_two_calls() -> None:
    """Two consecutive ``correlate`` calls on the same CandidateSet return
    byte-equal lists (dataclass ``__eq__``) with identical
    ``correlation_id``, ``correlation_hash``, ``evidence``, and
    ``created_at``.

    **Validates: Requirements 10.5, 27.7** (Property 17)
    """

    cve = _make_cve_record()
    fp = _make_fp_record()
    candidates = _make_candidates([cve], [fp])
    pipeline = _make_pipeline()

    results_a = await pipeline.correlate(candidates)
    results_b = await pipeline.correlate(candidates)

    assert results_a == results_b, "results must be byte-equal across calls"
    assert len(results_a) == 1
    for a, b in zip(results_a, results_b):
        assert a.correlation_id == b.correlation_id
        assert a.correlation_hash == b.correlation_hash
        assert a.evidence == b.evidence
        assert a.created_at == b.created_at
        assert a.record_a_hash == b.record_a_hash
        assert a.record_b_hash == b.record_b_hash
        assert a.pipeline_id == b.pipeline_id == "cve_correlation"


@given(
    cvss=st.floats(min_value=0.0, max_value=10.0, allow_nan=False),
    epss=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
    kev=st.booleans(),
    cve_suffix=st.text(
        alphabet="0123456789abcdef", min_size=15, max_size=15
    ),
    fp_suffix=st.text(
        alphabet="0123456789abcdef", min_size=15, max_size=15
    ),
)
@hyp_settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
async def test_property_determinism_pbt(
    cvss: float,
    epss: float,
    kev: bool,
    cve_suffix: str,
    fp_suffix: str,
) -> None:
    """Property-based: for any generated ``(cvss, epss, kev, hashes)`` tuple,
    two consecutive ``correlate`` calls produce byte-equal output.

    **Validates: Requirements 10.5, 27.7** (Property 17)
    """

    cve = _make_cve_record(
        raw_hash="c" + cve_suffix,
        cvss_v3_score=cvss,
        epss_score=epss,
        kev_listed=kev,
    )
    fp = _make_fp_record(raw_hash="d" + fp_suffix)
    candidates = _make_candidates([cve], [fp])
    pipeline = _make_pipeline()

    results_a = await pipeline.correlate(candidates)
    results_b = await pipeline.correlate(candidates)

    assert results_a == results_b


async def test_determinism_across_input_ordering() -> None:
    """Permuting record order within a tier yields byte-equal output.

    :meth:`CVEPipeline._partition` sorts records by ``raw_hash``
    internally, so the external ordering of the ``records`` lists inside
    :class:`CandidateSet` must not affect the final output.

    **Validates: Requirements 10.5, 27.7** (Property 17)
    """

    cve1 = _make_cve_record(
        raw_hash="a" * 16, cve_id="CVE-2024-1001", cvss_v3_score=7.5
    )
    cve2 = _make_cve_record(
        raw_hash="b" * 16, cve_id="CVE-2024-1002", cvss_v3_score=9.1
    )
    fp1 = _make_fp_record(raw_hash="c" * 16)
    fp2 = _make_fp_record(raw_hash="d" * 16)

    candidates_a = _make_candidates([cve1, cve2], [fp1, fp2])
    candidates_b = _make_candidates([cve2, cve1], [fp2, fp1])

    pipeline = _make_pipeline()
    results_a = await pipeline.correlate(candidates_a)
    results_b = await pipeline.correlate(candidates_b)

    assert results_a == results_b
    # Sanity: the cross-product produced 2 CVEs * 2 fingerprints = 4 rows.
    assert len(results_a) == 4


# ---------------------------------------------------------------------------
# Property 18 — confidence formula
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cvss, expected",
    [
        (0.0, 0.5),
        (1.0, 0.6),
        (3.0, 0.8),
        (5.0, 1.0),
        (7.0, 1.0),  # clamped at 1.0
        (10.0, 1.0),  # clamped at 1.0
    ],
)
async def test_property_confidence_formula_boundaries(
    cvss: float, expected: float
) -> None:
    """Boundary values for the confidence formula.

    ``confidence == min(1.0, 0.5 + 0.1 * cvss_v3_score)``

    **Validates: Requirements 10.3** (Property 18)
    """

    cve = _make_cve_record(cvss_v3_score=cvss)
    fp = _make_fp_record()
    candidates = _make_candidates([cve], [fp])
    pipeline = _make_pipeline()

    results = await pipeline.correlate(candidates)
    assert len(results) == 1
    assert math.isclose(results[0].confidence, expected, abs_tol=1e-9)


async def test_property_confidence_missing_cvss_treated_as_zero() -> None:
    """A missing ``cvss_v3_score`` is coerced to ``0.0`` → confidence ``0.5``.

    **Validates: Requirements 10.3** (Property 18)
    """

    cve = _make_cve_record(cvss_v3_score=None)
    fp = _make_fp_record()
    candidates = _make_candidates([cve], [fp])
    pipeline = _make_pipeline()

    results = await pipeline.correlate(candidates)
    assert len(results) == 1
    assert math.isclose(results[0].confidence, 0.5, abs_tol=1e-9)


@given(cvss=st.floats(min_value=0.0, max_value=10.0, allow_nan=False))
@hyp_settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
async def test_property_confidence_formula_pbt(cvss: float) -> None:
    """For any ``cvss_v3_score`` in ``[0.0, 10.0]``, the emitted confidence
    equals ``min(1.0, 0.5 + 0.1 * cvss)``.

    **Validates: Requirements 10.3** (Property 18)
    """

    cve = _make_cve_record(cvss_v3_score=cvss)
    fp = _make_fp_record()
    candidates = _make_candidates([cve], [fp])
    pipeline = _make_pipeline()

    results = await pipeline.correlate(candidates)
    assert len(results) == 1
    expected = min(1.0, 0.5 + 0.1 * cvss)
    assert math.isclose(results[0].confidence, expected, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# Supporting behaviour (edge cases, evidence shape, sort order)
# ---------------------------------------------------------------------------


async def test_empty_candidates_yields_empty() -> None:
    """No CVE records or no fingerprint records → empty output list."""

    pipeline = _make_pipeline()

    # No fingerprint records.
    cve = _make_cve_record()
    candidates_no_fp = _make_candidates([cve], [])
    assert await pipeline.correlate(candidates_no_fp) == []

    # No CVE records.
    fp = _make_fp_record()
    candidates_no_cve = _make_candidates([], [fp])
    assert await pipeline.correlate(candidates_no_cve) == []

    # Completely empty.
    empty = CandidateSet(
        pipeline_id="cve_correlation",
        source_tiers=[16, 17, 28, 29],
        time_window_start="2024-01-01T00:00:00+00:00",
        time_window_end="2024-01-31T00:00:00+00:00",
        records={},
        total_records=0,
    )
    assert await pipeline.correlate(empty) == []


async def test_evidence_contains_expected_keys_when_kev_and_epss_present() -> None:
    """When ``epss_score`` is present and ``kev_listed`` is ``True``, evidence
    carries ``cpe_match``, ``cvss_v3_score``, ``epss_score``, and
    ``kev_listed``.
    """

    cve = _make_cve_record(cvss_v3_score=9.5, epss_score=0.8, kev_listed=True)
    fp = _make_fp_record()
    candidates = _make_candidates([cve], [fp])
    pipeline = _make_pipeline()

    results = await pipeline.correlate(candidates)
    assert len(results) == 1
    evidence = results[0].evidence
    assert set(evidence.keys()) == {
        "cpe_match",
        "cvss_v3_score",
        "epss_score",
        "kev_listed",
    }
    assert evidence["kev_listed"] is True
    assert math.isclose(evidence["epss_score"], 0.8, abs_tol=1e-9)
    assert math.isclose(evidence["cvss_v3_score"], 9.5, abs_tol=1e-9)
    assert evidence["cpe_match"] == ["httpd"]


async def test_evidence_excludes_kev_and_epss_when_absent() -> None:
    """When ``kev_listed`` is ``False`` and ``epss_score`` is ``None``,
    evidence carries only ``cpe_match`` and ``cvss_v3_score``.

    The pipeline's ``_build_result`` conditionally inserts those keys —
    this test pins the contract so downstream consumers can rely on
    the presence check.
    """

    cve = _make_cve_record(cvss_v3_score=6.0, epss_score=None, kev_listed=False)
    fp = _make_fp_record()
    candidates = _make_candidates([cve], [fp])
    pipeline = _make_pipeline()

    results = await pipeline.correlate(candidates)
    assert len(results) == 1
    evidence = results[0].evidence
    assert set(evidence.keys()) == {"cpe_match", "cvss_v3_score"}


async def test_sort_order_design_3_4() -> None:
    """Results sort by ``(cvss DESC, kev DESC, epss DESC, record_a_hash ASC)``
    per Design §3.4.

    Seeds four CVEs whose ``(cvss, kev, epss, raw_hash)`` tuples exercise
    each level of the tiebreak. Each CVE matches the same fingerprint so
    the cross-product collapses to one row per CVE.
    """

    # Tuple layout: (raw_hash, cvss, kev, epss). Hashes are shared-prefix
    # so the record_a_hash tiebreak is exercised when the other three
    # columns match.
    cve_high_kev = _make_cve_record(
        raw_hash="1" * 16,
        cve_id="CVE-2024-AAAA",
        cvss_v3_score=9.0,
        kev_listed=True,
        epss_score=0.5,
    )
    cve_high_no_kev = _make_cve_record(
        raw_hash="2" * 16,
        cve_id="CVE-2024-BBBB",
        cvss_v3_score=9.0,
        kev_listed=False,
        epss_score=0.9,
    )
    cve_low_a = _make_cve_record(
        raw_hash="3" * 16,
        cve_id="CVE-2024-CCCC",
        cvss_v3_score=5.0,
        kev_listed=False,
        epss_score=0.3,
    )
    cve_low_b = _make_cve_record(
        raw_hash="4" * 16,
        cve_id="CVE-2024-DDDD",
        cvss_v3_score=5.0,
        kev_listed=False,
        epss_score=0.3,
    )
    fp = _make_fp_record()
    candidates = _make_candidates(
        [cve_high_kev, cve_high_no_kev, cve_low_a, cve_low_b], [fp]
    )

    pipeline = _make_pipeline()
    results = await pipeline.correlate(candidates)

    assert len(results) == 4
    # Order expected by Design §3.4:
    #   1. cvss=9.0, kev=True  → cve_high_kev (kev beats no-kev at same cvss)
    #   2. cvss=9.0, kev=False → cve_high_no_kev
    #   3. cvss=5.0, kev=False, epss=0.3, hash=3... → cve_low_a (hash ASC)
    #   4. cvss=5.0, kev=False, epss=0.3, hash=4... → cve_low_b
    expected_order = [
        "1" * 16,  # cve_high_kev
        "2" * 16,  # cve_high_no_kev
        "3" * 16,  # cve_low_a
        "4" * 16,  # cve_low_b
    ]
    assert [r.record_a_hash for r in results] == expected_order


async def test_determinism_correlation_id_uuid5() -> None:
    """The deterministic UUID5 ``correlation_id`` depends only on
    ``(cve.raw_hash, fp_record.raw_hash)``, not on wall-clock time.

    This is a corollary of Property 17 but worth pinning explicitly since
    the :class:`CorrelationEngine` uses ``correlation_id`` to detect
    already-persisted rows.
    """

    cve = _make_cve_record(raw_hash="e" * 16)
    fp = _make_fp_record(raw_hash="f" * 16)
    candidates = _make_candidates([cve], [fp])
    pipeline = _make_pipeline()

    results_a = await pipeline.correlate(candidates)
    # Build a fresh pipeline to avoid any per-instance caching.
    results_b = await _make_pipeline().correlate(candidates)

    assert len(results_a) == 1
    assert len(results_b) == 1
    assert results_a[0].correlation_id == results_b[0].correlation_id
    assert results_a[0].correlation_hash == results_b[0].correlation_hash
