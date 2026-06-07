"""Pydantic-settings based configuration for HYDRA."""

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from hydra.mil_int.settings import MilIntSettings


class DatabaseSettings(BaseModel):
    """Connection strings and pool settings for all storage engines."""

    postgres_dsn: str = "postgresql+asyncpg://hydra:hydra@localhost:5432/hydra"
    influxdb_url: str = "http://localhost:8086"
    influxdb_org: str = "hydra"
    influxdb_bucket: str = "hydra-timeseries"
    elasticsearch_url: str = "http://localhost:9200"
    neo4j_uri: str = "bolt://localhost:7687"
    minio_url: str = "http://localhost:9000"
    redis_url: str = "redis://localhost:6379/0"
    pg_pool_min: int = 5
    pg_pool_max: int = 20
    redis_pool_max: int = 20
    writer_batch_size: int = 100
    writer_poll_interval: float = 0.5
    writer_max_retries: int = 3
    reconciliation_interval: float = 300.0
    reconciliation_max_retries: int = 5
    reconciliation_alert_threshold: int = 100


class SchedulerSettings(BaseModel):
    """Airflow and scheduling configuration."""

    airflow_home: str = "/opt/airflow"
    retry_count: int = 3
    retry_delay_seconds: int = 60
    global_rate_limit: int = 100

    # P8: Concurrency (§6)
    global_concurrency_limit: int = 10
    cadence_concurrency_limits: Dict[str, int] = {
        "sub_minute": 4,
        "realtime": 3,
        "15min": 3,
        "hourly": 4,
        "daily": 6,
        "weekly": 4,
        "monthly_plus": 2,
    }

    # P8: Backpressure (§7)
    backpressure_soft_limit: int = 1_000
    backpressure_hard_limit: int = 5_000
    backpressure_wait_timeout: float = 60.0
    backpressure_poll_interval: float = 5.0
    engine_backpressure_overrides: Dict[str, Dict[str, int]] = {}

    # P8: Dead stream detection (§8)
    dead_stream_threshold: int = 5

    # P8: Staleness windows for monthly_plus DAG (§4.4)
    staleness_windows: Dict[str, int] = {
        "annual": 335,
        "quarterly": 85,
        "monthly": 28,
        "varies": 28,
    }

    # P8: Rate limit retry multiplier (§10.3)
    rate_limit_retry_delay_multiplier: float = 3.0


class CorrelationSettings(BaseModel):
    """Correlation configuration — nested under HydraSettings.correlation."""

    # Geospatial-Temporal Pipeline
    geo_temporal_radius_km: float = 50.0
    geo_temporal_window_s: float = 3600.0  # 1 hour

    # Entity Network Pipeline
    entity_name_similarity_threshold: float = 0.85
    entity_min_tag_overlap: int = 2
    entity_min_shared_keywords: int = 3

    # Threat Convergence Pipeline
    threat_convergence_window_s: float = 86400.0  # 24 hours
    threat_convergence_multiplier: float = 1.2
    threat_convergence_min_tiers: int = 3

    # Trigger settings
    min_trigger_interval_s: float = 300.0  # 5 minutes
    max_lookback_s: float = 86400.0  # 24 hour cap

    # Scheduled run lookback windows
    scheduled_lookback: Dict[str, float] = {
        "geospatial_temporal": 7200.0,    # 2 hours
        "entity_network": 172800.0,       # 48 hours
        "threat_convergence": 86400.0,    # 24 hours
    }

    # General
    confidence_threshold_default: float = 0.5
    max_pairs_per_run: int = 100_000
    max_results_per_run: int = 10_000


class ThreatLevelThresholds(BaseModel):
    """Configurable thresholds for threat level classification."""

    moderate_min_tiers: int = 2
    moderate_min_confidence: float = 0.4
    high_min_tiers: int = 3
    high_min_confidence: float = 0.5
    critical_min_tiers: int = 4
    critical_min_confidence: float = 0.7
    critical_temporal_window_s: float = 86400.0


class AnalysisSettings(BaseModel):
    """Analysis configuration — nested under HydraSettings.analysis."""

    # SITREP
    sitrep_max_events_per_tier: int = 20
    sitrep_significance_threshold: float = 0.3
    sitrep_domain_groups: Dict[str, List[int]] = {
        "Geophysical & Environmental": [1, 2, 3, 24, 25],
        "Security & Conflict": [6, 15, 16, 19, 20],
        "Economic & Governance": [5, 8, 9, 10, 11, 12, 13],
        "Health & Human Rights": [7, 21, 26],
        "Space & Science": [4, 22, 23],
        "Infrastructure & Energy": [18, 27],
        "Open Source & Social": [14, 17, 28],
    }

    # Entity Dossier
    dossier_network_depth: int = 2
    dossier_max_network_nodes: int = 50
    dossier_lookback_days: int = 365

    # Threat Assessment
    threat_level_thresholds: ThreatLevelThresholds = ThreatLevelThresholds()
    threat_min_convergence_tiers: int = 2

    # Timeline
    timeline_cluster_window_s: float = 3600.0
    timeline_max_events: int = 500

    # General
    default_max_records: int = 10_000
    product_dedup_enabled: bool = True

    # Watchlists
    entity_watchlist: List[Dict[str, str]] = []
    region_watchlist: List[str] = []


class APISettings(BaseModel):
    """API layer configuration — nested under HydraSettings.api."""

    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 4
    cors_origins: List[str] = ["*"]

    # Rate limiting
    rate_limit_enabled: bool = True
    rate_limit_read: int = 100
    rate_limit_read_burst: int = 20
    rate_limit_search: int = 30
    rate_limit_search_burst: int = 10
    rate_limit_write: int = 10
    rate_limit_write_burst: int = 5

    # Job management
    job_ttl_seconds: int = 3600
    job_redis_prefix: str = "hydra:job"

    # Pagination
    default_page_size: int = 50
    max_page_size: int = 500

    # Timeouts
    product_generation_timeout_s: int = 300
    correlation_run_timeout_s: int = 600

    # Timeseries constraints
    timeseries_max_raw_days: int = 30
    timeseries_max_agg_days: int = 365

    # Timeline constraints
    timeline_max_days: int = 90

    api_prefix: str = "/api/v1"


class MonitoringSettings(BaseModel):
    """Monitoring & alerting configuration — nested under HydraSettings.monitoring.

    All parameters support environment variable overrides via the
    ``HYDRA_MONITORING__`` prefix (e.g. ``HYDRA_MONITORING__SCHEDULER_COLLECTOR_INTERVAL=15``).
    See P12 design.md §"Component 1: MonitoringSettings".
    """

    # Collector intervals (seconds) — Requirement 21.1
    scheduler_collector_interval: float = 30.0
    storage_collector_interval: float = 30.0
    api_collector_interval: float = 60.0
    pipeline_collector_interval: float = 300.0

    # Anomaly detection — Requirement 21.2
    anomaly_detection_interval: float = 300.0
    anomaly_zscore_threshold: float = 3.0
    anomaly_ewma_span: int = 24
    anomaly_window_size: int = 288  # 24h of 5-min samples

    # Capacity planning — Requirement 21.3
    capacity_planning_interval: float = 3600.0
    capacity_pg_threshold_bytes: int = 100 * 1024**3      # 100 GB
    capacity_es_threshold_bytes: int = 50 * 1024**3       # 50 GB
    capacity_influx_threshold_bytes: int = 50 * 1024**3   # 50 GB
    capacity_minio_threshold_bytes: int = 500 * 1024**3   # 500 GB
    capacity_history_retention_days: int = 90

    # SLO targets — Requirement 21.4
    slo_adapter_success_target: float = 0.995
    slo_api_availability_target: float = 0.999
    slo_api_latency_p95_target: float = 0.99
    slo_api_latency_threshold_seconds: float = 2.0
    slo_product_generation_target: float = 0.99
    slo_ingestion_freshness_target: float = 0.98
    slo_storage_availability_target: float = 0.999
    slo_window_days: int = 30
    slo_short_window_days: int = 7

    # Structured logging
    log_format: str = "json"
    log_level: str = "INFO"

    # Prometheus
    metrics_path: str = "/metrics"
    scrape_interval_seconds: int = 15
    retention_days: int = 15


class HydraSettings(BaseSettings):
    """Root configuration composing all nested settings."""

    model_config = SettingsConfigDict(
        env_prefix="HYDRA_",
        env_nested_delimiter="__",
    )

    database: DatabaseSettings = DatabaseSettings()
    scheduler: SchedulerSettings = SchedulerSettings()
    correlation: CorrelationSettings = CorrelationSettings()
    analysis: AnalysisSettings = AnalysisSettings()
    api: APISettings = APISettings()
    monitoring: MonitoringSettings = MonitoringSettings()
    mil_int: MilIntSettings = MilIntSettings()
    stream_registry_path: Path = Path("src/hydra/registry/stream_registry.yaml")
    credential_store_path: Path = Path("config/credentials.yaml")
    data_dir: Path = Path("data")
    http_timeout_seconds: int = 30
    credentials: Dict[str, Dict[str, str]] = {}


def _load_yaml_config(path: Path) -> Dict[str, Any]:
    """Load a YAML configuration file."""
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def get_settings(config_path: Optional[Path] = None) -> HydraSettings:
    """Merge YAML config with environment variable overrides."""
    yaml_path = config_path or Path("config/settings.yaml")
    yaml_data = _load_yaml_config(yaml_path)
    return HydraSettings(**yaml_data)


settings: HydraSettings = get_settings()
