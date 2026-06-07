"""Prometheus metrics for the mil_int surface.

All metrics live in the default registry — :mod:`hydra.monitoring.metrics`
already exposes ``/metrics`` so any counter / histogram declared here is
scraped without additional wiring.
"""

from __future__ import annotations

try:
    from prometheus_client import Counter, Histogram
except ImportError:  # pragma: no cover - prometheus_client is a hard dep at runtime

    class _NoopMetric:
        def labels(self, *_args, **_kwargs):  # noqa: D401, ANN001
            return self

        def inc(self, *_args, **_kwargs):  # noqa: D401, ANN001
            return None

        def observe(self, *_args, **_kwargs):  # noqa: D401, ANN001
            return None

    Counter = Histogram = lambda *_a, **_kw: _NoopMetric()  # type: ignore[assignment]


hydra_mil_int_documents_indexed_total = Counter(
    "hydra_mil_int_documents_indexed_total",
    "Documents accepted by the mil_int surface and forwarded to storage.",
    labelnames=("tier", "country", "content_type"),
)

hydra_mil_int_access_policy_violations_total = Counter(
    "hydra_mil_int_access_policy_violations_total",
    "Records rejected by the mil_int surface due to access-policy or "
    "classification gating.",
    labelnames=("kind", "marker"),
)

hydra_mil_int_xref_resolutions_total = Counter(
    "hydra_mil_int_xref_resolutions_total",
    "Successful standards cross-reference resolutions.",
    labelnames=("from_family", "to_family"),
)

hydra_mil_int_freshness_score = Histogram(
    "hydra_mil_int_freshness_score",
    "Distribution of freshness scores assigned to ingested mil_int records.",
    buckets=(0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0),
)

hydra_mil_int_dedup_dropped_total = Counter(
    "hydra_mil_int_dedup_dropped_total",
    "Records dropped by the mil_int mirror-dedup resolver in favour of an "
    "authoritative source.",
    labelnames=("dropped_source", "canonical_source"),
)


__all__ = [
    "hydra_mil_int_documents_indexed_total",
    "hydra_mil_int_access_policy_violations_total",
    "hydra_mil_int_xref_resolutions_total",
    "hydra_mil_int_freshness_score",
    "hydra_mil_int_dedup_dropped_total",
]
