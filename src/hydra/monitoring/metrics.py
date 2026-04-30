"""HYDRA custom Prometheus metric definitions (P12).

All HYDRA application-level metrics exposed on ``/metrics`` are declared in
this module. HTTP request metrics (``http_requests_total``,
``http_request_duration_seconds``, ``http_requests_in_progress``) are
provided automatically by ``prometheus_fastapi_instrumentator`` and are
therefore *not* declared here.

Organization follows design.md §5 (Metric Definitions):

* §5.1  Adapter metrics            (9)
* §5.2  Scheduler metrics          (5)
* §5.3  Storage metrics            (9)
* §5.4  Backpressure metrics       (3)
* §5.5  API metrics                (5)
* §5.6  Intelligence product       (6)
* §5.7  Correlation metrics        (6)
* §5.8  Anomaly detection          (3)
* §5.9  Capacity planning          (9)
* §5.10 SLO metrics                (6)

Plus one internal metric, ``COLLECTOR_ERRORS``, used by
``BaseCollector._loop()`` to track collector failures without crashing
the background loops (see design §"Algorithm 1" and requirement 4.2).

Naming convention: every custom metric follows the pattern
``hydra_{subsystem}_{name}_{unit}`` — validated by Property 1
(Requirement 3.2). Label sets are intentionally kept low-cardinality
(Requirement 3.3): ``tier``, ``cadence``, ``engine``, ``stream_id``
(capped to the active ``StreamRegistry``), ``pipeline_id``,
``product_type``, ``endpoint``, ``method``, ``status_code``.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# Histogram bucket constants (design §5)
# ---------------------------------------------------------------------------

#: Adapter fetch duration buckets (seconds). Covers fast polling adapters up
#: to long-running bulk downloads.
ADAPTER_FETCH_BUCKETS: tuple[float, ...] = (
    0.1,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
)

#: Storage write batch duration buckets (seconds). Tuned for sub-second
#: storage engine writes with headroom for batch flushes.
STORAGE_WRITE_BUCKETS: tuple[float, ...] = (
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)

#: Intelligence product generation duration buckets (seconds).
PRODUCT_GENERATION_BUCKETS: tuple[float, ...] = (
    1.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
)

#: Score distribution buckets (0.0–1.0 in 0.1 increments) used for both
#: confidence and completeness histograms.
SCORE_BUCKETS: tuple[float, ...] = (
    0.0,
    0.1,
    0.2,
    0.3,
    0.4,
    0.5,
    0.6,
    0.7,
    0.8,
    0.9,
    1.0,
)

#: Records-per-product histogram buckets.
PRODUCT_RECORD_COUNT_BUCKETS: tuple[float, ...] = (
    10.0,
    50.0,
    100.0,
    500.0,
    1000.0,
    5000.0,
    10000.0,
)


# ---------------------------------------------------------------------------
# §5.1 Adapter metrics (9)
# ---------------------------------------------------------------------------

hydra_adapter_fetch_total = Counter(
    "hydra_adapter_fetch_total",
    "Total adapter fetch attempts. status is one of: success, failed, skipped.",
    labelnames=("stream_id", "tier", "adapter_type", "status"),
)

hydra_adapter_fetch_duration_seconds = Histogram(
    "hydra_adapter_fetch_duration_seconds",
    "Adapter fetch duration in seconds, from request start to response received.",
    labelnames=("stream_id", "tier", "adapter_type"),
    buckets=ADAPTER_FETCH_BUCKETS,
)

hydra_adapter_records_fetched_total = Counter(
    "hydra_adapter_records_fetched_total",
    "Total records fetched from upstream sources (before dedup/normalization).",
    labelnames=("stream_id", "tier"),
)

hydra_adapter_records_stored_total = Counter(
    "hydra_adapter_records_stored_total",
    "Total records successfully persisted to storage engines.",
    labelnames=("stream_id", "tier"),
)

hydra_adapter_records_deduplicated_total = Counter(
    "hydra_adapter_records_deduplicated_total",
    "Total records dropped by deduplication (Redis hit or PG unique constraint).",
    labelnames=("stream_id", "tier"),
)

hydra_adapter_fallback_total = Counter(
    "hydra_adapter_fallback_total",
    "Fallback adapter activations (primary source failed, secondary engaged).",
    labelnames=("stream_id", "tier"),
)

hydra_adapter_health_status = Gauge(
    "hydra_adapter_health_status",
    "Current adapter health status per stream: 0=UNREACHABLE, 1=DEGRADED, 2=OK.",
    labelnames=("stream_id", "tier"),
)

hydra_adapter_consecutive_failures = Gauge(
    "hydra_adapter_consecutive_failures",
    "Current consecutive failure count per stream (used for dead-stream detection).",
    labelnames=("stream_id",),
)

hydra_adapter_dead_streams = Gauge(
    "hydra_adapter_dead_streams",
    "Total count of streams currently classified as dead (consecutive failures exceed threshold).",
)


# ---------------------------------------------------------------------------
# §5.2 Scheduler metrics (5)
# ---------------------------------------------------------------------------

hydra_scheduler_active_adapters = Gauge(
    "hydra_scheduler_active_adapters",
    "Current number of running adapter executions across all cadences.",
)

hydra_scheduler_active_by_cadence = Gauge(
    "hydra_scheduler_active_by_cadence",
    "Active adapters per cadence (sub_minute, realtime, 15min, hourly, daily, weekly, monthly_plus).",
    labelnames=("cadence",),
)

hydra_scheduler_concurrency_limit = Gauge(
    "hydra_scheduler_concurrency_limit",
    "Configured concurrency limit per cadence (static, useful for dashboards).",
    labelnames=("cadence",),
)

hydra_scheduler_sla_misses_total = Counter(
    "hydra_scheduler_sla_misses_total",
    "Total SLA miss events reported by the scheduler maintenance DAG.",
    labelnames=("dag_id", "cadence"),
)

hydra_scheduler_health_status = Gauge(
    "hydra_scheduler_health_status",
    "Scheduler health status: 0=UNREACHABLE, 1=DEGRADED, 2=OK.",
)


# ---------------------------------------------------------------------------
# §5.3 Storage metrics (9)
# ---------------------------------------------------------------------------

hydra_storage_health_status = Gauge(
    "hydra_storage_health_status",
    "Per-engine storage health status: 0=UNREACHABLE, 1=DEGRADED, 2=OK.",
    labelnames=("engine",),
)

hydra_storage_health_latency_seconds = Gauge(
    "hydra_storage_health_latency_seconds",
    "Per-engine health check round-trip latency in seconds.",
    labelnames=("engine",),
)

hydra_storage_waq_depth = Gauge(
    "hydra_storage_waq_depth",
    "Write-ahead queue (WAQ) depth per storage engine.",
    labelnames=("engine",),
)

hydra_storage_dlq_depth = Gauge(
    "hydra_storage_dlq_depth",
    "Dead letter queue (DLQ) depth per storage engine.",
    labelnames=("engine",),
)

hydra_storage_write_total = Counter(
    "hydra_storage_write_total",
    "Total storage write attempts. status is one of: success, failed, deduplicated.",
    labelnames=("engine", "status"),
)

hydra_storage_write_duration_seconds = Histogram(
    "hydra_storage_write_duration_seconds",
    "Storage write batch duration in seconds (per engine).",
    labelnames=("engine",),
    buckets=STORAGE_WRITE_BUCKETS,
)

hydra_storage_dedup_redis_total = Counter(
    "hydra_storage_dedup_redis_total",
    "Records caught by Redis dedup (hash set hit before write).",
    labelnames=("tier",),
)

hydra_storage_dedup_pg_fallback_total = Counter(
    "hydra_storage_dedup_pg_fallback_total",
    "Records caught by PostgreSQL unique constraint (Redis miss, PG fallback).",
    labelnames=("tier",),
)

hydra_storage_records_total = Gauge(
    "hydra_storage_records_total",
    "Total persisted record count grouped by tier and storage status.",
    labelnames=("tier", "storage_status"),
)


# ---------------------------------------------------------------------------
# §5.4 Backpressure metrics (3)
# ---------------------------------------------------------------------------

hydra_backpressure_state = Gauge(
    "hydra_backpressure_state",
    "Current backpressure state per engine: 0=CLEAR, 1=THROTTLED, 2=BLOCKED.",
    labelnames=("engine",),
)

hydra_backpressure_soft_limit = Gauge(
    "hydra_backpressure_soft_limit",
    "Configured soft backpressure limit per engine (static snapshot of settings).",
    labelnames=("engine",),
)

hydra_backpressure_hard_limit = Gauge(
    "hydra_backpressure_hard_limit",
    "Configured hard backpressure limit per engine (static snapshot of settings).",
    labelnames=("engine",),
)


# ---------------------------------------------------------------------------
# §5.5 API metrics (5 custom — HTTP instrumentator metrics are separate)
# ---------------------------------------------------------------------------

hydra_api_rate_limit_hits_total = Counter(
    "hydra_api_rate_limit_hits_total",
    "Total rate-limit 429 responses. tier is one of: read, search, write.",
    labelnames=("tier", "api_key_name"),
)

hydra_api_job_status = Gauge(
    "hydra_api_job_status",
    "Current async job count grouped by status (pending, running, completed, failed).",
    labelnames=("status",),
)

hydra_api_job_duration_seconds = Histogram(
    "hydra_api_job_duration_seconds",
    "Async job completion duration in seconds. job_type e.g. product_generation, correlation_run.",
    labelnames=("job_type",),
)

hydra_api_active_keys = Gauge(
    "hydra_api_active_keys",
    "Count of active (non-expired, enabled) API keys.",
)

hydra_api_error_total = Counter(
    "hydra_api_error_total",
    "Total API errors grouped by error code and endpoint.",
    labelnames=("error_code", "endpoint"),
)


# ---------------------------------------------------------------------------
# §5.6 Intelligence product metrics (6)
# ---------------------------------------------------------------------------

hydra_product_generated_total = Counter(
    "hydra_product_generated_total",
    "Total intelligence products generated, grouped by product type and classification.",
    labelnames=("product_type", "classification"),
)

hydra_product_generation_duration_seconds = Histogram(
    "hydra_product_generation_duration_seconds",
    "Intelligence product generation duration in seconds.",
    labelnames=("product_type",),
    buckets=PRODUCT_GENERATION_BUCKETS,
)

hydra_product_confidence_score = Histogram(
    "hydra_product_confidence_score",
    "Distribution of intelligence product confidence scores (0.0–1.0).",
    labelnames=("product_type",),
    buckets=SCORE_BUCKETS,
)

hydra_product_completeness_score = Histogram(
    "hydra_product_completeness_score",
    "Distribution of intelligence product completeness scores (0.0–1.0).",
    labelnames=("product_type",),
    buckets=SCORE_BUCKETS,
)

hydra_product_record_count = Histogram(
    "hydra_product_record_count",
    "Distribution of underlying record counts per generated product.",
    labelnames=("product_type",),
    buckets=PRODUCT_RECORD_COUNT_BUCKETS,
)

hydra_product_source_tiers = Gauge(
    "hydra_product_source_tiers",
    "Average number of distinct source tiers contributing to products of a given type.",
    labelnames=("product_type",),
)


# ---------------------------------------------------------------------------
# §5.7 Correlation metrics (6)
# ---------------------------------------------------------------------------

hydra_correlation_total = Counter(
    "hydra_correlation_total",
    "Total correlation results emitted by a pipeline (including updates to existing correlations).",
    labelnames=("pipeline_id",),
)

hydra_correlation_new_total = Counter(
    "hydra_correlation_new_total",
    "New correlations emitted (excludes updates to pre-existing correlation rows).",
    labelnames=("pipeline_id",),
)

hydra_correlation_confidence = Histogram(
    "hydra_correlation_confidence",
    "Distribution of correlation confidence scores (0.0–1.0) per pipeline.",
    labelnames=("pipeline_id",),
    buckets=SCORE_BUCKETS,
)

hydra_correlation_run_duration_seconds = Histogram(
    "hydra_correlation_run_duration_seconds",
    "Correlation pipeline run duration in seconds.",
    labelnames=("pipeline_id",),
)

hydra_correlation_pairs_evaluated_total = Counter(
    "hydra_correlation_pairs_evaluated_total",
    "Total candidate record pairs evaluated by a correlation pipeline.",
    labelnames=("pipeline_id",),
)

hydra_correlation_tier_pair_total = Counter(
    "hydra_correlation_tier_pair_total",
    "Correlations emitted grouped by the pair of source tiers they link.",
    labelnames=("tier_a", "tier_b"),
)


# ---------------------------------------------------------------------------
# §5.8 Anomaly detection metrics (3)
# ---------------------------------------------------------------------------

hydra_anomaly_correlation_volume_zscore = Gauge(
    "hydra_anomaly_correlation_volume_zscore",
    "Z-score of correlation volume vs rolling baseline, per pipeline.",
    labelnames=("pipeline_id",),
)

hydra_anomaly_confidence_drift_zscore = Gauge(
    "hydra_anomaly_confidence_drift_zscore",
    "Z-score of mean correlation confidence vs rolling baseline, per pipeline.",
    labelnames=("pipeline_id",),
)

hydra_anomaly_flag = Gauge(
    "hydra_anomaly_flag",
    "Anomaly flag: 1 if anomaly detected this cycle, 0 otherwise. detector is e.g. "
    "correlation_volume or confidence_drift.",
    labelnames=("detector", "pipeline_id"),
)


# ---------------------------------------------------------------------------
# §5.9 Capacity planning metrics (9)
# ---------------------------------------------------------------------------

hydra_capacity_pg_size_bytes = Gauge(
    "hydra_capacity_pg_size_bytes",
    "Total PostgreSQL database size in bytes (pg_database_size).",
)

hydra_capacity_pg_table_size_bytes = Gauge(
    "hydra_capacity_pg_table_size_bytes",
    "Per-table PostgreSQL size in bytes (pg_total_relation_size) for key tables "
    "such as normalized_records, correlation_results, intelligence_products.",
    labelnames=("table",),
)

hydra_capacity_es_index_size_bytes = Gauge(
    "hydra_capacity_es_index_size_bytes",
    "Per-index Elasticsearch size in bytes, from /_cat/indices.",
    labelnames=("index",),
)

hydra_capacity_influx_bucket_size_bytes = Gauge(
    "hydra_capacity_influx_bucket_size_bytes",
    "InfluxDB bucket size in bytes (aggregate over configured HYDRA buckets).",
)

hydra_capacity_minio_bucket_size_bytes = Gauge(
    "hydra_capacity_minio_bucket_size_bytes",
    "Per-bucket MinIO size in bytes (sum over recursive list_objects).",
    labelnames=("bucket",),
)

hydra_capacity_pg_growth_rate_bytes_per_day = Gauge(
    "hydra_capacity_pg_growth_rate_bytes_per_day",
    "Linear-regression slope of PostgreSQL size over the 7-day snapshot window "
    "(bytes per day).",
)

hydra_capacity_days_to_threshold = Gauge(
    "hydra_capacity_days_to_threshold",
    "Projected days until the configured storage threshold is reached, per engine. "
    "A value of -1 means no exhaustion is projected; 0 means the threshold is already exceeded.",
    labelnames=("engine",),
)

hydra_capacity_ingestion_rate_records_per_minute = Gauge(
    "hydra_capacity_ingestion_rate_records_per_minute",
    "Current ingestion throughput in records per minute, per cadence.",
    labelnames=("cadence",),
)

hydra_capacity_query_latency_p95_seconds = Gauge(
    "hydra_capacity_query_latency_p95_seconds",
    "95th percentile query latency trend in seconds, per engine.",
    labelnames=("engine",),
)


# ---------------------------------------------------------------------------
# §5.10 SLO metrics (6)
# ---------------------------------------------------------------------------

hydra_slo_target = Gauge(
    "hydra_slo_target",
    "Configured SLO target value (e.g., 0.995 = 99.5%), per SLO.",
    labelnames=("slo_name",),
)

hydra_slo_current = Gauge(
    "hydra_slo_current",
    "Current SLI value for the SLO (0.0–1.0).",
    labelnames=("slo_name",),
)

hydra_slo_error_budget_remaining = Gauge(
    "hydra_slo_error_budget_remaining",
    "Remaining error budget in the SLO window (minutes, can go negative when breached).",
    labelnames=("slo_name",),
)

hydra_slo_burn_rate_1h = Gauge(
    "hydra_slo_burn_rate_1h",
    "1-hour error-budget burn rate (error_rate_1h / (1 - target)).",
    labelnames=("slo_name",),
)

hydra_slo_burn_rate_6h = Gauge(
    "hydra_slo_burn_rate_6h",
    "6-hour error-budget burn rate (error_rate_6h / (1 - target)).",
    labelnames=("slo_name",),
)

hydra_slo_breached = Gauge(
    "hydra_slo_breached",
    "SLO breach flag: 1 if error budget is exhausted, 0 otherwise.",
    labelnames=("slo_name",),
)


# ---------------------------------------------------------------------------
# Internal: collector error tracking (not part of the 61 custom metrics but
# required by BaseCollector._loop() — see design §"Algorithm 1" and
# Requirement 4.2 / Property 2).
# ---------------------------------------------------------------------------

COLLECTOR_ERRORS = Counter(
    "hydra_monitoring_collector_errors_total",
    "Total exceptions raised by BaseCollector.collect() implementations, "
    "caught and logged by the background loop without crashing the application.",
    labelnames=("collector",),
)


__all__ = [
    # Histogram bucket constants
    "ADAPTER_FETCH_BUCKETS",
    "STORAGE_WRITE_BUCKETS",
    "PRODUCT_GENERATION_BUCKETS",
    "SCORE_BUCKETS",
    "PRODUCT_RECORD_COUNT_BUCKETS",
    # §5.1 Adapter
    "hydra_adapter_fetch_total",
    "hydra_adapter_fetch_duration_seconds",
    "hydra_adapter_records_fetched_total",
    "hydra_adapter_records_stored_total",
    "hydra_adapter_records_deduplicated_total",
    "hydra_adapter_fallback_total",
    "hydra_adapter_health_status",
    "hydra_adapter_consecutive_failures",
    "hydra_adapter_dead_streams",
    # §5.2 Scheduler
    "hydra_scheduler_active_adapters",
    "hydra_scheduler_active_by_cadence",
    "hydra_scheduler_concurrency_limit",
    "hydra_scheduler_sla_misses_total",
    "hydra_scheduler_health_status",
    # §5.3 Storage
    "hydra_storage_health_status",
    "hydra_storage_health_latency_seconds",
    "hydra_storage_waq_depth",
    "hydra_storage_dlq_depth",
    "hydra_storage_write_total",
    "hydra_storage_write_duration_seconds",
    "hydra_storage_dedup_redis_total",
    "hydra_storage_dedup_pg_fallback_total",
    "hydra_storage_records_total",
    # §5.4 Backpressure
    "hydra_backpressure_state",
    "hydra_backpressure_soft_limit",
    "hydra_backpressure_hard_limit",
    # §5.5 API
    "hydra_api_rate_limit_hits_total",
    "hydra_api_job_status",
    "hydra_api_job_duration_seconds",
    "hydra_api_active_keys",
    "hydra_api_error_total",
    # §5.6 Intelligence product
    "hydra_product_generated_total",
    "hydra_product_generation_duration_seconds",
    "hydra_product_confidence_score",
    "hydra_product_completeness_score",
    "hydra_product_record_count",
    "hydra_product_source_tiers",
    # §5.7 Correlation
    "hydra_correlation_total",
    "hydra_correlation_new_total",
    "hydra_correlation_confidence",
    "hydra_correlation_run_duration_seconds",
    "hydra_correlation_pairs_evaluated_total",
    "hydra_correlation_tier_pair_total",
    # §5.8 Anomaly
    "hydra_anomaly_correlation_volume_zscore",
    "hydra_anomaly_confidence_drift_zscore",
    "hydra_anomaly_flag",
    # §5.9 Capacity
    "hydra_capacity_pg_size_bytes",
    "hydra_capacity_pg_table_size_bytes",
    "hydra_capacity_es_index_size_bytes",
    "hydra_capacity_influx_bucket_size_bytes",
    "hydra_capacity_minio_bucket_size_bytes",
    "hydra_capacity_pg_growth_rate_bytes_per_day",
    "hydra_capacity_days_to_threshold",
    "hydra_capacity_ingestion_rate_records_per_minute",
    "hydra_capacity_query_latency_p95_seconds",
    # §5.10 SLO
    "hydra_slo_target",
    "hydra_slo_current",
    "hydra_slo_error_budget_remaining",
    "hydra_slo_burn_rate_1h",
    "hydra_slo_burn_rate_6h",
    "hydra_slo_breached",
    # Internal
    "COLLECTOR_ERRORS",
]
