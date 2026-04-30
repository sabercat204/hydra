# Requirements Document

## Introduction

This document defines the requirements for P12 Monitoring & Alerting, the observability layer of the HYDRA OSINT Platform. P12 provides Prometheus-compatible metrics exposition, background metric collectors, alert rule definitions, Grafana dashboards, SLO/SLI tracking with error budget burn rates, statistical anomaly detection, and capacity planning with storage growth projection. Requirements are derived from the approved design document and follow EARS patterns with INCOSE quality standards.

## Glossary

- **Monitoring_Subsystem**: The complete P12 monitoring module (`src/hydra/monitoring/`) including collectors, anomaly detection, capacity planning, and SLO computation
- **Setup_Monitoring**: The `setup_monitoring()` entry point function that wires monitoring into the FastAPI application lifecycle
- **Metrics_Registry**: The `prometheus_client` default registry containing all registered HYDRA custom metrics
- **Metrics_Endpoint**: The `/metrics` HTTP endpoint on the FastAPI application that exposes Prometheus exposition format data
- **BaseCollector**: Abstract base class providing the async background loop pattern for metric collectors
- **SchedulerCollector**: Background collector scraping scheduler, adapter health, concurrency, dead streams, and SLA miss metrics
- **StorageCollector**: Background collector scraping storage engine health, WAQ/DLQ depths, and backpressure state
- **APICollector**: Background collector scraping API job states, rate limit consumption, and API key statistics
- **PipelineCollector**: Background collector scraping intelligence product and correlation metrics from PostgreSQL
- **AnomalyDetector**: Statistical anomaly detection component using z-score and EWMA methods on correlation volume and confidence drift
- **CapacityPlanner**: Storage growth projection component using linear regression on storage size snapshots
- **SLOComputer**: Component that computes SLO status, error budgets, and burn rates for 6 defined SLOs
- **MonitoringContext**: Dataclass holding references to all monitoring components for lifecycle management
- **Prometheus**: Self-hosted Prometheus v2.51.0 instance scraping the Metrics_Endpoint
- **Alertmanager**: Self-hosted Prometheus Alertmanager v0.27.0 routing firing alerts to receivers
- **Grafana**: Self-hosted Grafana OSS 10.4.0 providing dashboard visualization
- **MonitoringSettings**: Pydantic configuration model nested under `HydraSettings.monitoring`
- **SLODefinition**: Dataclass defining an SLO with name, SLI metric, target, window, and burn rate thresholds
- **SLOStatus**: Dataclass representing computed SLO state including error budget and burn rates
- **Capacity_Snapshot**: A row in the `capacity_snapshots` PostgreSQL table recording engine storage size at a point in time

## Requirements

### Requirement 1: Monitoring Initialization

**User Story:** As a platform operator, I want monitoring to initialize automatically when the FastAPI application starts, so that observability is available from the moment the system is running.

#### Acceptance Criteria

1. WHEN the FastAPI application starts, THE Setup_Monitoring function SHALL instrument the application with `prometheus_fastapi_instrumentator` providing `http_requests_total`, `http_request_duration_seconds`, and `http_requests_in_progress` metrics
2. WHEN the FastAPI application starts, THE Setup_Monitoring function SHALL mount the Metrics_Endpoint at the `/metrics` path
3. WHEN the FastAPI application starts, THE Setup_Monitoring function SHALL create and start four collector background tasks (SchedulerCollector, StorageCollector, APICollector, PipelineCollector) as `asyncio.Task` instances
4. WHEN the FastAPI application starts, THE Setup_Monitoring function SHALL initialize and start the AnomalyDetector and CapacityPlanner background tasks
5. WHEN the FastAPI application starts, THE Setup_Monitoring function SHALL initialize the SLOComputer with 6 SLO definitions loaded from MonitoringSettings
6. WHEN initialization completes, THE Setup_Monitoring function SHALL return a MonitoringContext containing references to all collectors, the AnomalyDetector, the CapacityPlanner, the SLOComputer, and all background tasks

### Requirement 2: Metrics Endpoint Exposition

**User Story:** As a platform operator, I want a Prometheus-compatible metrics endpoint, so that Prometheus can scrape HYDRA's operational metrics.

#### Acceptance Criteria

1. THE Metrics_Endpoint SHALL respond to HTTP GET requests with status code 200 and Content-Type `text/plain`
2. THE Metrics_Endpoint SHALL return all registered metrics from the Metrics_Registry in Prometheus exposition format
3. THE Metrics_Endpoint SHALL exclude the `/metrics` and `/api/v1/health/ping` paths from HTTP request instrumentation
4. WHEN Prometheus scrapes the Metrics_Endpoint at 15-second intervals, THE Metrics_Endpoint SHALL return the latest cached metric values from the Metrics_Registry

### Requirement 3: Metric Registration and Naming

**User Story:** As a platform operator, I want all HYDRA metrics to follow a consistent naming convention, so that metrics are discoverable and self-documenting.

#### Acceptance Criteria

1. WHEN Setup_Monitoring completes, THE Metrics_Registry SHALL contain all 47 custom metrics defined in the metric definitions (adapter, scheduler, storage, backpressure, API, intelligence product, correlation, anomaly, capacity, and SLO metrics)
2. THE Metrics_Registry SHALL name each custom metric following the pattern `hydra_{subsystem}_{name}_{unit}` where subsystem is one of: adapter, storage, scheduler, api, correlation, product, dlq, backpressure, anomaly, capacity, slo
3. THE Metrics_Registry SHALL restrict metric labels to low-cardinality values: `tier`, `cadence`, `engine`, `stream_id` (capped to active streams from StreamRegistry), `pipeline_id`, `product_type`, `endpoint`, `method`, `status_code`

### Requirement 4: Collector Background Loop

**User Story:** As a platform operator, I want metric collectors to run continuously in the background, so that metrics are kept up-to-date without manual intervention.

#### Acceptance Criteria

1. WHILE a BaseCollector is running, THE BaseCollector SHALL execute its `collect()` method repeatedly with a sleep of `interval` seconds between each execution
2. IF a BaseCollector's `collect()` method raises an exception, THEN THE BaseCollector SHALL log the error, increment the `COLLECTOR_ERRORS` counter metric with the collector name label, and continue the collection loop
3. WHEN a BaseCollector's `stop()` method is called, THE BaseCollector SHALL set `_running` to False causing the loop to exit after the current sleep completes
4. WHEN a BaseCollector's `start()` method is called, THE BaseCollector SHALL return an `asyncio.Task` running the collection loop

### Requirement 5: Scheduler Metric Collection

**User Story:** As a platform operator, I want scheduler and adapter health metrics collected automatically, so that I can monitor ingestion pipeline health.

#### Acceptance Criteria

1. WHEN the SchedulerCollector executes a collection cycle, THE SchedulerCollector SHALL call `SchedulerHealthAggregator.check()` and update `hydra_scheduler_health_status` with the result (0=UNREACHABLE, 1=DEGRADED, 2=OK)
2. WHEN the SchedulerCollector executes a collection cycle, THE SchedulerCollector SHALL read `ConcurrencyManager.active_count` and update `hydra_scheduler_active_adapters`
3. WHEN the SchedulerCollector executes a collection cycle, THE SchedulerCollector SHALL read `active_by_cadence` for each cadence and update `hydra_scheduler_active_by_cadence` with the cadence label
4. WHEN the SchedulerCollector executes a collection cycle, THE SchedulerCollector SHALL scan Redis keys matching `hydra:stream_failures:*` and update `hydra_adapter_consecutive_failures` per stream
5. WHEN the SchedulerCollector executes a collection cycle, THE SchedulerCollector SHALL count streams exceeding the `dead_stream_threshold` and update `hydra_adapter_dead_streams`
6. WHEN the SchedulerCollector executes a collection cycle, THE SchedulerCollector SHALL scan Redis keys matching `hydra:sla_miss:*` for new events since the last collection and increment `hydra_scheduler_sla_misses_total`
7. WHEN the SchedulerCollector executes a collection cycle, THE SchedulerCollector SHALL update `hydra_adapter_health_status` per stream from the adapter health data

### Requirement 6: Storage Metric Collection

**User Story:** As a platform operator, I want storage engine health and queue depth metrics collected automatically, so that I can monitor data pipeline health and detect backpressure.

#### Acceptance Criteria

1. WHEN the StorageCollector executes a collection cycle, THE StorageCollector SHALL call `StorageHealthAggregator.check_all()` and update `hydra_storage_health_status` and `hydra_storage_health_latency_seconds` per engine
2. WHEN the StorageCollector executes a collection cycle, THE StorageCollector SHALL read queue depths for each engine and update `hydra_storage_waq_depth`
3. WHEN the StorageCollector executes a collection cycle, THE StorageCollector SHALL read DLQ depths for each engine and update `hydra_storage_dlq_depth`
4. WHEN the StorageCollector executes a collection cycle, THE StorageCollector SHALL call `BackpressureMonitor.check()` and update `hydra_backpressure_state` per engine (0=CLEAR, 1=THROTTLED, 2=BLOCKED)
5. WHEN the StorageCollector executes a collection cycle, THE StorageCollector SHALL set `hydra_backpressure_soft_limit` and `hydra_backpressure_hard_limit` per engine from MonitoringSettings

### Requirement 7: API Metric Collection

**User Story:** As a platform operator, I want API job state and rate limit metrics collected automatically, so that I can monitor API health and usage patterns.

#### Acceptance Criteria

1. WHEN the APICollector executes a collection cycle, THE APICollector SHALL scan Redis keys matching `hydra:job:*`, count jobs by status (pending, running, completed, failed), and update `hydra_api_job_status`
2. WHEN the APICollector executes a collection cycle, THE APICollector SHALL query the `api_keys` PostgreSQL table for active non-expired keys and update `hydra_api_active_keys`

### Requirement 8: Pipeline Metric Collection

**User Story:** As a platform operator, I want intelligence product and correlation metrics collected automatically, so that I can monitor analytical pipeline performance.

#### Acceptance Criteria

1. WHEN the PipelineCollector executes a collection cycle, THE PipelineCollector SHALL query `intelligence_products` for products generated since the last collection and increment `hydra_product_generated_total` by product type
2. WHEN the PipelineCollector executes a collection cycle, THE PipelineCollector SHALL observe confidence and completeness score distributions into `hydra_product_confidence_score` and `hydra_product_completeness_score` histograms
3. WHEN the PipelineCollector executes a collection cycle, THE PipelineCollector SHALL query `correlation_results` for new correlations since the last collection and increment `hydra_correlation_total` by pipeline
4. WHEN the PipelineCollector executes a collection cycle, THE PipelineCollector SHALL query `normalized_records` grouped by tier and storage_status and update `hydra_storage_records_total`


### Requirement 9: Z-Score Anomaly Detection

**User Story:** As a platform operator, I want statistical anomaly detection on correlation metrics, so that I am alerted to unusual patterns that may indicate data quality issues or upstream changes.

#### Acceptance Criteria

1. WHEN the AnomalyDetector executes a detection cycle, THE AnomalyDetector SHALL query PostgreSQL for correlation volume and mean confidence per pipeline for the last 5 minutes
2. WHEN the AnomalyDetector computes a z-score, THE AnomalyDetector SHALL append the current value to a bounded rolling history (deque with maxlen equal to `window_size`)
3. WHILE the rolling history for a metric key contains fewer than 30 data points, THE AnomalyDetector SHALL return z-score 0.0 and anomaly flag False
4. WHILE the standard deviation of the rolling history is zero, THE AnomalyDetector SHALL return z-score 0.0 and anomaly flag False
5. WHEN the absolute z-score exceeds the configured `zscore_threshold`, THE AnomalyDetector SHALL set `hydra_anomaly_flag` to 1 for the corresponding detector and pipeline
6. WHEN the absolute z-score is within the configured `zscore_threshold`, THE AnomalyDetector SHALL set `hydra_anomaly_flag` to 0 for the corresponding detector and pipeline

### Requirement 10: EWMA Anomaly Detection

**User Story:** As a platform operator, I want EWMA-based anomaly detection complementing z-score, so that trend-sensitive anomalies are detected even when the rolling window is noisy.

#### Acceptance Criteria

1. WHEN the AnomalyDetector computes EWMA, THE AnomalyDetector SHALL use alpha = 2.0 / (ewma_span + 1) as the smoothing factor
2. WHEN the AnomalyDetector encounters the first observation for a metric key, THE AnomalyDetector SHALL initialize the EWMA state to the current value and return deviation 0.0 and anomaly flag False
3. WHEN the AnomalyDetector computes EWMA deviation, THE AnomalyDetector SHALL calculate `|current_value - ewma| / stdev` and flag as anomaly when the deviation exceeds `zscore_threshold`
4. WHILE the rolling history for a metric key contains fewer than 30 data points, THE AnomalyDetector SHALL return deviation 0.0 and anomaly flag False for EWMA checks

### Requirement 11: Anomaly Detection History Bounds

**User Story:** As a platform operator, I want anomaly detection memory usage bounded, so that long-running detection does not consume unbounded memory.

#### Acceptance Criteria

1. THE AnomalyDetector SHALL maintain rolling history per metric key using a deque with maxlen equal to `window_size` (default 288)
2. THE AnomalyDetector SHALL update `hydra_anomaly_correlation_volume_zscore` and `hydra_anomaly_confidence_drift_zscore` gauge metrics after each detection cycle

### Requirement 12: Linear Regression Growth Projection

**User Story:** As a platform operator, I want storage growth projections based on historical data, so that I can plan capacity before storage is exhausted.

#### Acceptance Criteria

1. WHEN the CapacityPlanner computes growth projection, THE CapacityPlanner SHALL perform linear regression on (timestamp, size_bytes) pairs from the 7-day history window
2. WHILE the history for an engine contains fewer than 3 data points, THE CapacityPlanner SHALL return growth rate 0.0 and days-to-threshold -1.0
3. WHILE the computed growth rate is zero or negative, THE CapacityPlanner SHALL return days-to-threshold -1.0 (no exhaustion projected)
4. WHEN the current storage size already exceeds the configured threshold, THE CapacityPlanner SHALL return days-to-threshold 0.0
5. WHEN growth rate is positive and current size is below threshold, THE CapacityPlanner SHALL compute days-to-threshold as `(threshold - current_size) / growth_rate_per_day`

### Requirement 13: Capacity Data Collection and Persistence

**User Story:** As a platform operator, I want storage sizes collected from all engines and persisted for trend analysis, so that growth projections are based on real historical data.

#### Acceptance Criteria

1. WHEN the CapacityPlanner executes a collection cycle, THE CapacityPlanner SHALL query PostgreSQL for database size via `pg_database_size()` and per-table sizes via `pg_total_relation_size()` for key tables
2. WHEN the CapacityPlanner executes a collection cycle, THE CapacityPlanner SHALL query Elasticsearch for index sizes via `/_cat/indices`
3. WHEN the CapacityPlanner executes a collection cycle, THE CapacityPlanner SHALL query InfluxDB for bucket usage
4. WHEN the CapacityPlanner executes a collection cycle, THE CapacityPlanner SHALL query MinIO for object sizes via recursive `list_objects`
5. WHEN the CapacityPlanner collects storage sizes, THE CapacityPlanner SHALL persist snapshots to the `capacity_snapshots` PostgreSQL table with engine, metric_name, value_bytes, and collected_at
6. WHEN the CapacityPlanner executes a cleanup cycle, THE CapacityPlanner SHALL delete rows from `capacity_snapshots` where `collected_at` is older than `capacity_history_retention_days` (default 90 days)
7. WHEN the CapacityPlanner completes a cycle, THE CapacityPlanner SHALL update `hydra_capacity_*_size_bytes` gauges, `hydra_capacity_pg_growth_rate_bytes_per_day`, and `hydra_capacity_days_to_threshold` per engine

### Requirement 14: SLO Computation

**User Story:** As a platform operator, I want SLO status with error budgets and burn rates computed automatically, so that I can track service reliability against defined targets.

#### Acceptance Criteria

1. THE SLOComputer SHALL maintain 6 SLO definitions: adapter_success_rate (99.5%, 30d), api_availability (99.9%, 30d), api_latency_p95 (99.0%, 30d), product_generation_success (99.0%, 7d), ingestion_freshness (98.0%, 7d), storage_availability (99.9%, 30d)
2. WHEN the SLOComputer computes an SLO, THE SLOComputer SHALL calculate error_budget_total as `(1 - target) * window_minutes`
3. WHEN the SLOComputer computes an SLO, THE SLOComputer SHALL calculate burn_rate_1h as `error_rate_1h / (1 - target)` and burn_rate_6h as `error_rate_6h / (1 - target)`
4. WHEN error_budget_remaining is less than or equal to zero, THE SLOComputer SHALL set `is_breached` to True on the SLOStatus
5. WHEN error_budget_remaining is greater than zero, THE SLOComputer SHALL set `is_breached` to False on the SLOStatus
6. WHEN the SLOComputer completes computation, THE SLOComputer SHALL update `hydra_slo_target`, `hydra_slo_current`, `hydra_slo_error_budget_remaining`, `hydra_slo_burn_rate_1h`, `hydra_slo_burn_rate_6h`, and `hydra_slo_breached` gauge metrics

### Requirement 15: Critical Alert Rules

**User Story:** As a platform operator, I want critical alerts that page immediately for system-threatening conditions, so that I can respond before data loss or total outage occurs.

#### Acceptance Criteria

1. WHEN `hydra_scheduler_health_status` equals 0 for 2 minutes, THE Prometheus alert rules SHALL fire `HydraSchedulerUnreachable` with severity critical
2. WHEN `hydra_storage_health_status` for postgres or redis equals 0 for 1 minute, THE Prometheus alert rules SHALL fire `HydraStoragePrimaryDown` with severity critical
3. WHEN `hydra_backpressure_state` equals 2 for 5 minutes, THE Prometheus alert rules SHALL fire `HydraBackpressureBlocked` with severity critical
4. WHEN `hydra_storage_dlq_depth` exceeds 500 for 10 minutes, THE Prometheus alert rules SHALL fire `HydraDLQCritical` with severity critical
5. WHEN `up{job="hydra-api"}` equals 0 for 1 minute, THE Prometheus alert rules SHALL fire `HydraAPIDown` with severity critical
6. WHEN `hydra_slo_burn_rate_1h` exceeds 14.4 AND `hydra_slo_burn_rate_6h` exceeds 6 for 5 minutes, THE Prometheus alert rules SHALL fire `HydraSLOBurnRateCritical` with severity critical

### Requirement 16: Warning Alert Rules

**User Story:** As a platform operator, I want warning alerts for degraded conditions, so that I can investigate and remediate before conditions become critical.

#### Acceptance Criteria

1. WHEN the adapter failure rate exceeds 30% for 15 minutes, THE Prometheus alert rules SHALL fire `HydraAdapterHighFailureRate` with severity warning
2. WHEN the async job failure rate exceeds 20% for 15 minutes, THE Prometheus alert rules SHALL fire `HydraJobFailureRate` with severity warning
3. WHEN the API 5xx error rate exceeds 5% for 5 minutes, THE Prometheus alert rules SHALL fire `HydraAPIErrorRate` with severity warning
4. WHEN `hydra_api_rate_limit_hits_total` rate exceeds 1 per second for 10 minutes, THE Prometheus alert rules SHALL fire `HydraRateLimitExhaustion` with severity warning
5. WHEN `hydra_anomaly_flag{detector="correlation_volume"}` equals 1 for 30 minutes, THE Prometheus alert rules SHALL fire `HydraAnomalyCorrelationVolume` with severity warning
6. WHEN `hydra_anomaly_flag{detector="confidence_drift"}` equals 1 for 30 minutes, THE Prometheus alert rules SHALL fire `HydraAnomalyConfidenceDrift` with severity warning
7. WHEN `hydra_capacity_days_to_threshold` is less than 30 for 1 hour, THE Prometheus alert rules SHALL fire `HydraCapacityStorageLow` with severity warning
8. WHEN `hydra_slo_burn_rate_1h` exceeds 3 AND `hydra_slo_burn_rate_6h` exceeds 1 for 30 minutes, THE Prometheus alert rules SHALL fire `HydraSLOBurnRateWarning` with severity warning

### Requirement 17: Alert Routing and Inhibition

**User Story:** As a platform operator, I want alerts routed to appropriate channels with intelligent suppression, so that critical alerts reach PagerDuty and warnings go to Slack without duplicate noise.

#### Acceptance Criteria

1. WHEN a critical alert fires, THE Alertmanager SHALL route the alert to the `pagerduty-critical` receiver and the `slack-critical` receiver
2. WHEN a warning alert fires, THE Alertmanager SHALL route the alert to the `slack-warning` receiver
3. WHEN a critical alert is firing for a given `alertname` and `engine` pair, THE Alertmanager SHALL inhibit the corresponding warning alert for the same `alertname` and `engine` pair
4. THE Alertmanager SHALL group alerts by `alertname` with a 30-second group wait and 5-minute group interval

### Requirement 18: Recording Rules

**User Story:** As a platform operator, I want pre-computed recording rules for expensive PromQL aggregations, so that dashboards load quickly and long-term metric retention is efficient.

#### Acceptance Criteria

1. THE Prometheus recording rules SHALL compute 5-minute aggregations including `hydra:adapter_success_rate_5m`, `hydra:adapter_fetch_duration_p95_5m`, `hydra:storage_write_duration_p95_5m`, `hydra:api_request_duration_p95_5m`, `hydra:ingestion_rate_5m`, `hydra:api_error_rate_5m`, and `hydra:correlation_rate_5m`
2. THE Prometheus recording rules SHALL compute 1-hour aggregations including `hydra:adapter_success_rate_1h`, `hydra:adapter_fetch_duration_p95_1h`, `hydra:storage_write_duration_p95_1h`, `hydra:api_request_duration_p95_1h`, `hydra:ingestion_rate_1h`, `hydra:product_generation_rate_1h`, and `hydra:dlq_growth_rate_1h`
3. THE Prometheus recording rules SHALL prefix all computed metrics with `hydra:` and reference only defined source metrics

### Requirement 19: Alert Rule Validity

**User Story:** As a platform operator, I want all alert rules to reference valid metrics, so that alerts fire correctly and do not produce evaluation errors.

#### Acceptance Criteria

1. THE Prometheus alert rules SHALL reference only metrics that are (a) registered custom metrics in the Metrics_Registry, (b) metrics produced by `prometheus_fastapi_instrumentator`, or (c) Prometheus built-in metrics such as `up`
2. THE Prometheus alert rules SHALL pass `promtool check rules` syntax validation

### Requirement 20: Grafana Dashboard Provisioning

**User Story:** As a platform operator, I want Grafana dashboards provisioned automatically from version-controlled JSON files, so that dashboards are reproducible and consistent across environments.

#### Acceptance Criteria

1. WHEN Grafana starts, THE Grafana provisioning configuration SHALL configure Prometheus as the default datasource at `http://prometheus:9090`
2. WHEN Grafana starts, THE Grafana provisioning configuration SHALL load 5 dashboard JSON files from the provisioned dashboard directory into the HYDRA folder
3. THE Grafana dashboard definitions SHALL include: hydra_overview.json (system overview), hydra_adapters.json (adapter health), hydra_storage.json (storage engine health), hydra_api.json (API performance), hydra_intelligence.json (intelligence products and correlations)

### Requirement 21: Monitoring Configuration

**User Story:** As a platform operator, I want all monitoring parameters configurable via environment variables, so that I can tune monitoring behavior without code changes.

#### Acceptance Criteria

1. THE MonitoringSettings SHALL provide configurable collector intervals: scheduler (default 30s), storage (default 30s), API (default 60s), pipeline (default 300s)
2. THE MonitoringSettings SHALL provide configurable anomaly detection parameters: detection interval (default 300s), z-score threshold (default 3.0), EWMA span (default 24), window size (default 288)
3. THE MonitoringSettings SHALL provide configurable capacity planning parameters: planning interval (default 3600s), per-engine thresholds (PG 100GB, ES 50GB, InfluxDB 50GB, MinIO 500GB), history retention (default 90 days)
4. THE MonitoringSettings SHALL provide configurable SLO targets for all 6 SLOs with default values matching the SLO definitions
5. THE MonitoringSettings SHALL support environment variable overrides via the `HYDRA_MONITORING__` prefix

### Requirement 22: Error Handling and Resilience

**User Story:** As a platform operator, I want the monitoring subsystem to be resilient to transient failures, so that monitoring continues operating even when upstream dependencies are temporarily unavailable.

#### Acceptance Criteria

1. IF a collector's upstream dependency (Redis, PostgreSQL, SchedulerHealthAggregator) is temporarily unreachable during `collect()`, THEN THE BaseCollector SHALL log the error and continue the collection loop on the next interval
2. IF the AnomalyDetector fails to query PostgreSQL during a detection cycle, THEN THE AnomalyDetector SHALL log the AnomalyDetectionError and continue on the next interval
3. IF the CapacityPlanner fails to query one or more storage engines during a collection cycle, THEN THE CapacityPlanner SHALL skip the failed engine, log the CapacityPlanningError, and continue with available engines
4. IF the SLOComputer fails to compute an SLI metric, THEN THE SLOComputer SHALL report `current_value = 0.0` and `is_breached = True` as a conservative default
5. THE Monitoring_Subsystem SHALL define an exception hierarchy: MonitoringError (base), CollectorError, AnomalyDetectionError, CapacityPlanningError, SLOComputationError

### Requirement 23: Graceful Shutdown

**User Story:** As a platform operator, I want monitoring to shut down cleanly when the application stops, so that no orphaned background tasks remain.

#### Acceptance Criteria

1. WHEN `MonitoringContext.shutdown()` is called, THE MonitoringContext SHALL call `stop()` on each collector and cancel all background tasks
2. WHEN the FastAPI application lifespan exits, THE application SHALL call `MonitoringContext.shutdown()` to stop all monitoring background tasks

### Requirement 24: Capacity Snapshots Database Schema

**User Story:** As a platform operator, I want capacity snapshots persisted in a structured database table, so that growth projections have reliable historical data.

#### Acceptance Criteria

1. THE Monitoring_Subsystem SHALL create a `capacity_snapshots` table with columns: id (SERIAL PRIMARY KEY), engine (VARCHAR(32) NOT NULL), metric_name (VARCHAR(64) NOT NULL), value_bytes (BIGINT NOT NULL), collected_at (TIMESTAMPTZ NOT NULL DEFAULT NOW())
2. THE Monitoring_Subsystem SHALL create an index `idx_capacity_snapshots_engine_time` on (engine, collected_at DESC) for efficient time-range queries
3. THE `capacity_snapshots` table SHALL restrict `engine` values to: postgres, elasticsearch, influxdb, minio
4. THE `capacity_snapshots` table SHALL require `value_bytes` to be non-negative

### Requirement 25: Docker Compose Observability Stack

**User Story:** As a platform operator, I want the observability stack (Prometheus, Alertmanager, Grafana) defined in Docker Compose, so that the full monitoring infrastructure is deployable with a single command.

#### Acceptance Criteria

1. THE Docker Compose configuration SHALL define a `prometheus` service using image `prom/prometheus:v2.51.0` with volume mounts for `prometheus.yml` and rules directory, retention set to 15 days, and port 9090 exposed
2. THE Docker Compose configuration SHALL define an `alertmanager` service using image `prom/alertmanager:v0.27.0` with volume mount for `alertmanager.yml` and port 9093 exposed
3. THE Docker Compose configuration SHALL define a `grafana` service using image `grafana/grafana-oss:10.4.0` with volume mounts for provisioning and dashboards, environment-variable-based admin credentials, and port 3000 exposed
4. THE Prometheus scrape configuration SHALL target `hydra-api:8000` at the `/metrics` path with a 15-second scrape interval

### Requirement 26: Structured Logging

**User Story:** As a platform operator, I want structured JSON logging from the monitoring subsystem, so that logs are machine-parseable and compatible with log aggregation systems.

#### Acceptance Criteria

1. THE Monitoring_Subsystem SHALL emit all log messages as structured JSON to stdout
2. THE Monitoring_Subsystem SHALL include the following fields in log entries: timestamp, level, module, message, and contextual fields (stream_id, tier, engine, request_id, duration_ms, error) where applicable
