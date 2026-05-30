# Implementation Plan: P12 Monitoring & Alerting

## Overview

Implement the HYDRA observability layer in incremental steps: configuration and exceptions first, then metric definitions, collectors, SLO/anomaly/capacity modules, infrastructure configs (Prometheus, Alertmanager, Grafana), database migration, and finally wiring everything into the FastAPI application lifecycle. Each task builds on previous outputs. All code is Python 3.12+ async-first with Pydantic v2 models. Tests use mocked dependencies throughout.

## Tasks

- [x] 1. Foundation: Configuration, exceptions, and metric definitions
  - [x] 1.1 Create `src/hydra/monitoring/exceptions.py` with the exception hierarchy
    - Define `MonitoringError` (base), `CollectorError` (with `collector_name`), `AnomalyDetectionError`, `CapacityPlanningError`, `SLOComputationError`
    - _Requirements: 22.5_

  - [x] 1.2 Create `MonitoringSettings` model and add to `HydraSettings`
    - Create `src/hydra/monitoring/__init__.py` (initially empty, will export `setup_monitoring` later)
    - Add `MonitoringSettings` as a Pydantic `BaseModel` in a new section of `src/hydra/config.py` (or in `src/hydra/monitoring/__init__.py` and imported)
    - Include all configurable fields: collector intervals, anomaly detection params, capacity planning params, SLO targets, logging, Prometheus settings
    - Add `monitoring: MonitoringSettings = MonitoringSettings()` field to `HydraSettings`
    - Ensure `HYDRA_MONITORING__` env var prefix works via `env_nested_delimiter`
    - _Requirements: 21.1, 21.2, 21.3, 21.4, 21.5_

  - [x] 1.3 Create `src/hydra/monitoring/metrics.py` with all 47 custom metric definitions
    - Define all Counters, Gauges, and Histograms organized by subsystem: adapter (9), scheduler (5), storage (9), backpressure (3), API (5), product (6), correlation (6), anomaly (3), capacity (9), SLO (6)
    - Use `prometheus_client` library with correct types, label sets, and histogram buckets as specified in design §5
    - Follow `hydra_{subsystem}_{name}_{unit}` naming convention
    - Also define the internal `COLLECTOR_ERRORS` counter with `collector` label
    - _Requirements: 3.1, 3.2, 3.3_

  - [x]* 1.4 Write property test for metric naming convention
    - **Property 1: Metric Naming Convention**
    - Verify all registered custom metrics match `hydra_{subsystem}_{name}_{unit}` pattern
    - **Validates: Requirement 3.2**

  - [x]* 1.5 Write unit tests for metric registration (`tests/test_metrics.py` — registration subset)
    - Test all 47 metrics are registered in the prometheus_client registry
    - Test label cardinality constraints
    - Test counter increment, gauge set, histogram observe operations
    - _Requirements: 3.1, 3.2, 3.3_

- [x] 2. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Instrumentator and base collector
  - [x] 3.1 Create `src/hydra/monitoring/instrumentator.py`
    - Implement `create_instrumentator()` returning a configured `Instrumentator` instance
    - Exclude `/metrics` and `/api/v1/health/ping` from instrumentation
    - Implement `instrument_app(app)` that instruments and exposes `/metrics`
    - _Requirements: 1.1, 1.2, 2.1, 2.2, 2.3_

  - [x] 3.2 Create `src/hydra/monitoring/collectors/__init__.py` with `BaseCollector` abstract class
    - Implement `__init__(interval)`, `start() -> asyncio.Task`, `stop()`, `_loop()`, abstract `collect()`
    - `_loop()` catches all exceptions, logs them, increments `COLLECTOR_ERRORS` counter, and continues
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

  - [x]* 3.3 Write property test for collector error isolation
    - **Property 2: Collector Error Isolation**
    - Create a test collector that raises various exception types; verify the loop continues and error counter increments
    - **Validates: Requirements 4.2, 22.1**

- [x] 4. Implement concrete collectors
  - [x] 4.1 Create `src/hydra/monitoring/collectors/scheduler.py` — `SchedulerCollector`
    - Inject `SchedulerHealthAggregator`, `ConcurrencyManager`, `Redis`, `StreamRegistry`
    - Implement `collect()`: update health status, active adapters, active by cadence, consecutive failures, dead streams, SLA misses, per-stream health
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_

  - [x] 4.2 Create `src/hydra/monitoring/collectors/storage.py` — `StorageCollector`
    - Inject `StorageHealthAggregator`, `RedisCache`, `BackpressureMonitor`, `HydraSettings`
    - Implement `collect()`: update per-engine health/latency, WAQ depth, DLQ depth, backpressure state, soft/hard limits
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

  - [x] 4.3 Create `src/hydra/monitoring/collectors/api.py` — `APICollector`
    - Inject `Redis`, `asyncpg.Pool`
    - Implement `collect()`: scan `hydra:job:*` for job status counts, query `api_keys` for active key count
    - _Requirements: 7.1, 7.2_

  - [x] 4.4 Create `src/hydra/monitoring/collectors/pipeline.py` — `PipelineCollector`
    - Inject `asyncpg.Pool`
    - Implement `collect()`: query `intelligence_products`, observe confidence/completeness histograms, query `correlation_results`, query `normalized_records` by tier/status
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

  - [x]* 4.5 Write unit tests for all collectors (`tests/test_collectors.py`)
    - Mock all upstream dependencies (Redis, PostgreSQL, SchedulerHealthAggregator, etc.)
    - Test each collector updates correct metrics with expected values
    - Test error handling (collector continues after exception)
    - Test start/stop lifecycle and interval behavior
    - _Requirements: 5.1–5.7, 6.1–6.5, 7.1–7.2, 8.1–8.4, 4.1–4.4, 22.1_

- [x] 5. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. SLO computation
  - [x] 6.1 Create `src/hydra/monitoring/slo.py` — `SLODefinition`, `SLOStatus`, and `SLOComputer`
    - Define `SLODefinition` and `SLOStatus` dataclasses
    - Implement `SLOComputer` with 6 SLO definitions loaded from `MonitoringSettings`
    - Implement `compute_slo()`: error budget = `(1 - target) * window_minutes`, burn rates = `error_rate / (1 - target)`, breach flag
    - Implement `compute_all()` returning list of `SLOStatus`
    - On SLI query failure: report `current_value=0.0`, `is_breached=True`
    - Update `hydra_slo_*` gauge metrics after computation
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.6, 22.4_

  - [x]* 6.2 Write property test for error budget and burn rate computation
    - **Property 11: Error Budget and Burn Rate Computation**
    - For any target in (0,1) and positive window, verify budget = `(1-target)*window*24*60` and burn_rate = `error_rate/(1-target)`
    - **Validates: Requirements 14.2, 14.3**

  - [x]* 6.3 Write property test for SLO breach consistency
    - **Property 12: SLO Breach Consistency**
    - Verify `is_breached` is True iff `error_budget_remaining <= 0`; on query failure, `current_value=0.0` and `is_breached=True`
    - **Validates: Requirements 14.4, 14.5, 22.4**

  - [x]* 6.4 Write unit tests for SLO module (`tests/test_slo.py`)
    - Test SLO definition loading, target values, budget math, burn rates, breach flag, metrics exposure
    - _Requirements: 14.1–14.6_

- [x] 7. Anomaly detection
  - [x] 7.1 Create `src/hydra/monitoring/anomaly.py` — `AnomalyDetector`
    - Implement z-score detection with rolling history (`deque(maxlen=window_size)`)
    - Implement EWMA detection with `alpha = 2.0 / (ewma_span + 1)`
    - Minimum 30 data points before flagging; zero stdev returns (0.0, False)
    - Background loop querying PostgreSQL for correlation volume and confidence per pipeline
    - Update `hydra_anomaly_*` metrics after each cycle
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 10.1, 10.2, 10.3, 10.4, 11.1, 11.2, 22.2_

  - [x]* 7.2 Write property test for minimum samples guard
    - **Property 3: Anomaly Detection Minimum Samples Guard**
    - For any metric key with < 30 data points, both z-score and EWMA return (0.0, False)
    - **Validates: Requirements 9.3, 10.2, 10.4**

  - [x]* 7.3 Write property test for z-score zero variance safety
    - **Property 4: Z-Score Zero Variance Safety**
    - When all history values are identical, z-score returns (0.0, False)
    - **Validates: Requirement 9.4**

  - [x]* 7.4 Write property test for z-score anomaly flag consistency
    - **Property 5: Z-Score Anomaly Flag Consistency**
    - With ≥30 points and non-zero stdev, flag is 1 iff |z| > threshold
    - **Validates: Requirements 9.5, 9.6**

  - [x]* 7.5 Write property test for EWMA alpha computation
    - **Property 6: EWMA Alpha Computation**
    - For any positive span, alpha = 2/(span+1) and alpha ∈ (0,1)
    - **Validates: Requirement 10.1**

  - [x]* 7.6 Write property test for EWMA deviation detection
    - **Property 7: EWMA Deviation Detection**
    - With ≥30 points and non-zero stdev, flag set when |current - ewma|/stdev > threshold
    - **Validates: Requirement 10.3**

  - [x]* 7.7 Write property test for history window bounded
    - **Property 8: History Window Bounded**
    - For any number of appended values, history length never exceeds window_size
    - **Validates: Requirements 9.2, 11.1**

  - [x]* 7.8 Write unit tests for anomaly detection (`tests/test_anomaly.py`)
    - Test z-score and EWMA math with known inputs, minimum samples, zero stdev, flag set/clear, metrics update, history bounds
    - _Requirements: 9.1–9.6, 10.1–10.4, 11.1–11.2_

- [x] 8. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Capacity planning and database migration
  - [x] 9.1 Create `alembic/versions/p12_001_capacity_snapshots.py` migration
    - Create `capacity_snapshots` table: id (SERIAL PK), engine (VARCHAR(32) NOT NULL), metric_name (VARCHAR(64) NOT NULL), value_bytes (BIGINT NOT NULL), collected_at (TIMESTAMPTZ NOT NULL DEFAULT NOW())
    - Create index `idx_capacity_snapshots_engine_time` on (engine, collected_at DESC)
    - Add CHECK constraint for engine values and non-negative value_bytes
    - _Requirements: 24.1, 24.2, 24.3, 24.4_

  - [x] 9.2 Create `src/hydra/monitoring/capacity.py` — `CapacityPlanner`
    - Implement `_collect_storage_sizes()`: query PG (`pg_database_size`, `pg_total_relation_size`), ES (`/_cat/indices`), InfluxDB (bucket usage), MinIO (`list_objects`)
    - Implement `_project_growth()`: linear regression on 7-day window, returns (growth_rate, days_to_threshold)
    - Persist snapshots to `capacity_snapshots` table
    - Cleanup rows older than `capacity_history_retention_days`
    - Update `hydra_capacity_*` gauge metrics
    - Skip failed engines gracefully, log `CapacityPlanningError`
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7, 22.3_

  - [x]* 9.3 Write property test for growth projection correctness
    - **Property 9: Growth Projection Correctness**
    - Verify: <3 points → (0.0, -1.0); negative growth → days=-1.0; over threshold → days=0.0; otherwise days=(threshold-current)/rate
    - **Validates: Requirements 12.2, 12.3, 12.4, 12.5**

  - [x]* 9.4 Write property test for linear regression slope direction
    - **Property 10: Linear Regression Slope Direction**
    - Strictly increasing sequence → positive growth rate; strictly decreasing → negative
    - **Validates: Requirement 12.1**

  - [x]* 9.5 Write unit tests for capacity planning (`tests/test_capacity.py`)
    - Test PG/ES/MinIO size collection, linear regression, days-to-threshold, negative growth, min data points, snapshot persistence, retention cleanup, metrics update
    - _Requirements: 12.1–12.5, 13.1–13.7_

- [x] 10. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Prometheus and Alertmanager configuration
  - [x] 11.1 Create `prometheus/prometheus.yml` scrape configuration
    - Global scrape interval 15s, evaluation interval 15s
    - Rule files: `hydra_alerts.yml`, `hydra_recording.yml`
    - Alertmanager target: `alertmanager:9093`
    - Scrape target: `hydra-api:8000` at `/metrics` with 15s interval
    - _Requirements: 25.1, 25.4_

  - [x] 11.2 Create `prometheus/rules/hydra_alerts.yml` with all alert rules
    - 5 critical alerts: HydraSchedulerUnreachable, HydraStoragePrimaryDown, HydraBackpressureBlocked, HydraDLQCritical, HydraAPIDown
    - 2 SLO burn rate alerts: HydraSLOBurnRateCritical, HydraSLOBurnRateWarning
    - 8 warning alerts: HydraAdapterHighFailureRate, HydraJobFailureRate, HydraAPIErrorRate, HydraRateLimitExhaustion, HydraAnomalyCorrelationVolume, HydraAnomalyConfidenceDrift, HydraCapacityStorageLow, HydraSLOBurnRateWarning
    - All rules reference only valid metrics (custom, instrumentator, or built-in `up`)
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6, 16.1, 16.2, 16.3, 16.4, 16.5, 16.6, 16.7, 16.8, 19.1, 19.2_

  - [x] 11.3 Create `prometheus/rules/hydra_recording.yml` with recording rules
    - 9 rules at 5-minute interval: adapter_success_rate, fetch_duration_p95/p50, storage_write_duration_p95, api_request_duration_p95/p50, ingestion_rate, api_error_rate, correlation_rate
    - 7 rules at 1-hour interval: adapter_success_rate, fetch_duration_p95, storage_write_duration_p95, api_request_duration_p95, ingestion_rate, product_generation_rate, dlq_growth_rate
    - All prefixed with `hydra:`, referencing only defined source metrics
    - _Requirements: 18.1, 18.2, 18.3_

  - [x] 11.4 Create `alertmanager/alertmanager.yml` with routing and receivers
    - Route critical alerts to `pagerduty-critical` and `slack-critical` receivers
    - Route warning alerts to `slack-warning` receiver
    - Group by `alertname`, 30s group wait, 5m group interval
    - Inhibition: critical suppresses warning for same `alertname` + `engine`
    - Placeholder webhook URLs for Slack and PagerDuty
    - _Requirements: 17.1, 17.2, 17.3, 17.4_

  - [x]* 11.5 Write alert rule validation tests (`tests/test_alerts.py`)
    - Validate YAML syntax of alert and recording rule files
    - Verify all PromQL expressions reference valid metrics
    - Test routing rules and inhibition logic
    - Test critical alerts route to pagerduty, warnings to slack
    - **Property 13: Alert Rule Metric Validity** — all referenced metrics are valid
    - **Property 14: Recording Rule Consistency** — all output metrics prefixed `hydra:`, all source metrics defined
    - _Requirements: 15.1–15.6, 16.1–16.8, 17.1–17.4, 18.1–18.3, 19.1–19.2_

- [x] 12. Grafana dashboards and provisioning
  - [x] 12.1 Create `grafana/provisioning/datasources.yaml` and `grafana/provisioning/dashboards.yaml`
    - Datasource: Prometheus at `http://prometheus:9090`, default, not editable
    - Dashboard provider: HYDRA folder, file-based from `/var/lib/grafana/dashboards`
    - _Requirements: 20.1, 20.2_

  - [x] 12.2 Create `grafana/dashboards/hydra_overview.json`
    - 11 panels: System Health, Active Adapters, Dead Streams, Ingestion Rate, Adapter Success Rate, Storage Health Matrix, Backpressure State, WAQ Depth, DLQ Depth, SLO Error Budget, Capacity Forecast
    - _Requirements: 20.3_

  - [x] 12.3 Create `grafana/dashboards/hydra_adapters.json`
    - 10 panels: Fetch Duration P95/P50, Records Fetched/Stored Rate, Dedup Rate, Fallback Activations, Consecutive Failures, Health Status, Concurrency by Cadence, SLA Misses
    - _Requirements: 20.3_

  - [x] 12.4 Create `grafana/dashboards/hydra_storage.json`
    - 11 panels: Engine Health, Health Check Latency, Write Duration P95, Write Success Rate, WAQ Depth vs Limits, DLQ Depth, DLQ Growth Rate, Redis Dedup Hits, PG Fallback Dedup, Records by Status, Storage Sizes
    - _Requirements: 20.3_

  - [x] 12.5 Create `grafana/dashboards/hydra_api.json`
    - 11 panels: Request Rate, Request Duration P95/P50, Error Rate, Status Code Distribution, In-Flight Requests, Rate Limit Hits, Active API Keys, Job Status Distribution, Job Duration P95, API Errors
    - _Requirements: 20.3_

  - [x] 12.6 Create `grafana/dashboards/hydra_intelligence.json`
    - 11 panels: Products Generated, Generation Duration P95, Confidence/Completeness Distribution, Source Tier Coverage, Correlation Rate, Correlation Confidence, Tier Pair Frequency, Anomaly Flags, Correlation Volume Z-Score, Confidence Drift Z-Score
    - _Requirements: 20.3_

- [x] 13. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 14. Integration: Wire monitoring into FastAPI and Docker Compose
  - [x] 14.1 Create `src/hydra/monitoring/__init__.py` with `setup_monitoring()` and `MonitoringContext`
    - Implement `MonitoringContext` dataclass with `shutdown()` method
    - Implement `setup_monitoring()`: instrument app, mount `/metrics`, create and start all 4 collectors, start AnomalyDetector and CapacityPlanner background tasks, initialize SLOComputer, return `MonitoringContext`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 23.1_

  - [x] 14.2 Update `src/hydra/api/app.py` to call `setup_monitoring()` in lifespan
    - Call `setup_monitoring()` during startup, passing all required dependencies
    - Call `monitoring_ctx.shutdown()` during shutdown
    - _Requirements: 1.1, 1.2, 1.3, 23.2_

  - [x] 14.3 Update `docker-compose.yml` with Prometheus, Alertmanager, and Grafana services
    - Add `prometheus` service (prom/prometheus:v2.51.0) with volume mounts, 15d retention, port 9090
    - Add `alertmanager` service (prom/alertmanager:v0.27.0) with volume mount, port 9093
    - Add `grafana` service (grafana/grafana-oss:10.4.0) with provisioning/dashboard volumes, env-based admin creds, port 3000
    - Add `prometheus_data` and `grafana_data` volumes
    - _Requirements: 25.1, 25.2, 25.3_

  - [x]* 14.4 Write integration tests for metrics endpoint (`tests/test_metrics.py` — endpoint subset)
    - Test `GET /metrics` returns 200 with `text/plain`
    - Test response contains `hydra_` prefixed metrics
    - Test FastAPI instrumentator metrics present (`http_requests_total`, etc.)
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [x] 15. Structured logging configuration
  - [x] 15.1 Add structured JSON logging to the monitoring subsystem
    - Configure `logging` module to emit JSON to stdout with fields: timestamp, level, module, message, and contextual fields (stream_id, tier, engine, request_id, duration_ms, error)
    - Apply to all monitoring module loggers
    - _Requirements: 26.1, 26.2_

- [x] 16. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation after each major phase
- Property tests validate universal correctness properties from the design document
- All tests use mocked dependencies — no live connections required
- Do not modify upstream files (P0, P7, P8, P11) unless explicitly adding `MonitoringSettings` to `HydraSettings`
