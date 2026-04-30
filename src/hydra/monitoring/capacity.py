"""CapacityPlanner — storage growth projection and forecasting (P12 §10).

This module implements :class:`CapacityPlanner`, a periodic
:class:`~hydra.monitoring.collectors.BaseCollector` subclass that
forecasts storage exhaustion across the four HYDRA data engines
(PostgreSQL, Elasticsearch, InfluxDB, MinIO) by running linear
regression on a rolling 7-day window of size snapshots persisted in the
``capacity_snapshots`` PostgreSQL table.

Each cycle:

1. **Collect sizes** from every configured engine. Per-engine failures
   are isolated — the cycle skips the failed engine, logs a
   :class:`CapacityPlanningError`, and proceeds with the rest
   (Requirement 22.3).
2. **Persist snapshots** to ``capacity_snapshots`` so growth projections
   survive restarts (Requirement 13.5).
3. **Project growth** per engine by querying the last 7 days of
   snapshots and running least-squares linear regression (Requirements
   12.1–12.5).
4. **Update metrics** — the ``hydra_capacity_*_size_bytes`` gauges, the
   PostgreSQL growth-rate gauge, and the per-engine
   ``hydra_capacity_days_to_threshold`` gauge (Requirement 13.7).
5. **Cleanup** rows older than ``capacity_history_retention_days``
   (Requirement 13.6).

The ingestion-rate and query-latency gauges listed in design §5.9
(``hydra_capacity_ingestion_rate_records_per_minute``,
``hydra_capacity_query_latency_p95_seconds``) are populated by the
Prometheus recording rules defined in task 11.3, not by this
collector — they are derived from HTTP instrumentator metrics rather
than storage snapshots.

Pluggable backends
------------------

Rather than hard-coding ``elasticsearch-py`` / ``influxdb-client`` /
``boto3`` imports into the minimal monitoring dep set, the three
non-PostgreSQL backends are injected as :class:`typing.Protocol`
instances. Production wiring in :mod:`hydra.monitoring.__init__` will
supply thin adapters over the real clients; unit tests can pass
in-memory stubs. When a backend is ``None`` (default), the planner
silently skips the corresponding engine — useful for partial
deployments (e.g., a PG-only development environment).

The PostgreSQL pool is **mandatory** because the snapshot table is
PG-backed: without it there is no durable history to regress over.

Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 13.1, 13.2, 13.3, 13.4,
13.5, 13.6, 13.7, 22.3.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from hydra.config import MonitoringSettings
from hydra.monitoring.collectors import BaseCollector
from hydra.monitoring.exceptions import CapacityPlanningError
from hydra.monitoring.metrics import (
    hydra_capacity_days_to_threshold,
    hydra_capacity_es_index_size_bytes,
    hydra_capacity_influx_bucket_size_bytes,
    hydra_capacity_minio_bucket_size_bytes,
    hydra_capacity_pg_growth_rate_bytes_per_day,
    hydra_capacity_pg_size_bytes,
    hydra_capacity_pg_table_size_bytes,
)

if TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

#: Valid engine identifiers mirrored from the ``capacity_snapshots``
#: CHECK constraint (see ``alembic/versions/p12_001_capacity_snapshots.py``).
#: The regression query, metric labels, and persistence paths all use
#: these canonical names — keep them in sync with the migration.
ENGINE_POSTGRES: Final[str] = "postgres"
ENGINE_ELASTICSEARCH: Final[str] = "elasticsearch"
ENGINE_INFLUXDB: Final[str] = "influxdb"
ENGINE_MINIO: Final[str] = "minio"

#: Window over which the linear regression is computed (Requirement 12.1).
_REGRESSION_WINDOW_DAYS: Final[int] = 7

#: Key PostgreSQL tables whose size is reported as a per-table gauge.
#: Kept to a small low-cardinality set (Requirement 3.3); the full
#: database size is reported separately via ``pg_database_size``.
_PG_KEY_TABLES: Final[tuple[str, ...]] = (
    "normalized_records",
    "correlation_results",
    "intelligence_products",
)

#: Metric-name prefix used when persisting per-table PG snapshots. The
#: growth regression filters to ``metric_name = 'pg_database_size'`` so
#: per-table sizes must use a distinct prefix that does not collide.
_PG_TABLE_METRIC_PREFIX: Final[str] = "pg_table:"

#: Metric-name used for the headline PG database size — this is the
#: series the growth regression targets.
_PG_DATABASE_METRIC: Final[str] = "pg_database_size"

#: Metric-name prefixes for the non-PG engines (snapshot rows).
_ES_METRIC_PREFIX: Final[str] = "es_index:"
_INFLUX_METRIC: Final[str] = "influx_bucket"
_MINIO_METRIC_PREFIX: Final[str] = "minio_bucket:"


# ---------------------------------------------------------------------------
# Sentinel values used on hydra_capacity_days_to_threshold
# ---------------------------------------------------------------------------

#: ``days_to_threshold`` return value indicating "no exhaustion projected"
#: (fewer than 3 data points, zero/negative growth rate, or zero-variance
#: timestamps). Chosen to be distinguishable from any non-negative
#: day count on dashboards (Requirements 12.2, 12.3).
_NO_PROJECTION: Final[float] = -1.0

#: ``days_to_threshold`` return value indicating the threshold is
#: already exceeded (Requirement 12.4).
_ALREADY_OVER_THRESHOLD: Final[float] = 0.0


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_PG_DATABASE_SIZE_SQL: Final[str] = (
    "SELECT pg_database_size(current_database())::bigint AS size_bytes"
)

# Per-table size. Uses ``to_regclass`` so missing tables return NULL
# rather than raising — the planner then silently skips that table.
_PG_TABLE_SIZE_SQL: Final[str] = (
    "SELECT pg_total_relation_size(to_regclass($1))::bigint AS size_bytes"
)

_INSERT_SNAPSHOT_SQL: Final[str] = (
    "INSERT INTO capacity_snapshots (engine, metric_name, value_bytes) "
    "VALUES ($1, $2, $3)"
)

_SELECT_PG_HISTORY_SQL: Final[str] = (
    "SELECT collected_at, value_bytes "
    "FROM capacity_snapshots "
    "WHERE engine = $1 "
    "  AND metric_name = $2 "
    "  AND collected_at > NOW() - ($3::int * INTERVAL '1 day') "
    "ORDER BY collected_at ASC"
)

# Retention cleanup. Uses an integer day count parameterized into an
# INTERVAL expression — no string interpolation (safe against injection
# even though the value comes from settings).
_CLEANUP_SQL: Final[str] = (
    "DELETE FROM capacity_snapshots "
    "WHERE collected_at < NOW() - ($1::int * INTERVAL '1 day')"
)


# ---------------------------------------------------------------------------
# Pluggable backend protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class ESSizeBackend(Protocol):
    """Abstract backend returning per-index Elasticsearch sizes.

    Production adapters will wrap ``elasticsearch-py``'s ``/_cat/indices``
    endpoint. Tests may supply an in-memory stub returning a fixed
    ``{index_name: bytes}`` mapping.
    """

    async def fetch_index_sizes(self) -> dict[str, int]:
        """Return a ``{index_name: size_bytes}`` mapping for all HYDRA indices."""


@runtime_checkable
class InfluxSizeBackend(Protocol):
    """Abstract backend returning aggregate InfluxDB bucket usage.

    Production adapters will query the InfluxDB ``/api/v2/buckets``
    usage API. Tests may return a fixed integer.
    """

    async def fetch_bucket_size(self) -> int:
        """Return the total InfluxDB bucket size in bytes."""


@runtime_checkable
class MinIOSizeBackend(Protocol):
    """Abstract backend returning per-bucket MinIO sizes.

    Production adapters will wrap ``boto3``/``minio`` ``list_objects``
    with recursion. Tests may supply a fixed ``{bucket: bytes}`` mapping.
    """

    async def fetch_bucket_sizes(self) -> dict[str, int]:
        """Return a ``{bucket_name: size_bytes}`` mapping for all HYDRA buckets."""


# ---------------------------------------------------------------------------
# CapacityPlanner
# ---------------------------------------------------------------------------


class CapacityPlanner(BaseCollector):
    """Periodic storage growth projection and capacity forecasting.

    The planner inherits background-loop plumbing from
    :class:`BaseCollector`: any exception escaping :meth:`collect` is
    caught, logged, and counted against ``COLLECTOR_ERRORS`` while the
    loop continues on the next interval. Within :meth:`collect`, each
    per-engine operation is additionally wrapped so that a failure of
    one backend (e.g., Elasticsearch unreachable) does not prevent the
    others from reporting (Requirement 22.3).
    """

    def __init__(
        self,
        pg_pool: "asyncpg.Pool",
        settings: MonitoringSettings,
        es_backend: ESSizeBackend | None = None,
        influx_backend: InfluxSizeBackend | None = None,
        minio_backend: MinIOSizeBackend | None = None,
        interval: float | None = None,
    ) -> None:
        """Create the planner.

        Args:
            pg_pool: Async PostgreSQL connection pool. Required — used
                both for ``pg_database_size``/``pg_total_relation_size``
                queries and for persisting snapshots to
                ``capacity_snapshots``.
            settings: Monitoring configuration supplying the planning
                interval, per-engine thresholds, and retention window
                (Requirement 21.3).
            es_backend: Optional :class:`ESSizeBackend`. When ``None``
                (default), the Elasticsearch engine is skipped.
            influx_backend: Optional :class:`InfluxSizeBackend`. When
                ``None`` (default), InfluxDB is skipped.
            minio_backend: Optional :class:`MinIOSizeBackend`. When
                ``None`` (default), MinIO is skipped.
            interval: Optional override for the loop interval in
                seconds. When ``None``, the interval is taken from
                ``settings.capacity_planning_interval``.
        """
        super().__init__(
            interval=(
                interval
                if interval is not None
                else settings.capacity_planning_interval
            )
        )
        self._pg_pool = pg_pool
        self._settings = settings
        self._es_backend = es_backend
        self._influx_backend = influx_backend
        self._minio_backend = minio_backend

    # ------------------------------------------------------------------
    # Pure growth projection (static — exposed for property tests)
    # ------------------------------------------------------------------

    @staticmethod
    def _project_growth(
        snapshots: list[tuple[datetime, int]],
        threshold_bytes: int,
        current_bytes: int,
    ) -> tuple[float, float]:
        """Project storage growth via least-squares linear regression.

        Implements Algorithm 4 from the design document. The regression
        uses "days since the first snapshot" as the independent variable
        so the returned slope is already expressed in bytes-per-day and
        does not require a post-hoc unit conversion.

        Args:
            snapshots: Time-ordered ``(collected_at, value_bytes)`` pairs
                from the last ``_REGRESSION_WINDOW_DAYS`` days. Must be
                sorted ascending by timestamp.
            threshold_bytes: Exhaustion threshold for this engine
                (typically sourced from
                :class:`MonitoringSettings`). When ``current_bytes``
                already exceeds this value, days-to-threshold is
                reported as ``0.0`` (Requirement 12.4).
            current_bytes: Most recent observed size, used as the base
                for the ``(threshold - current) / rate`` calculation.

        Returns:
            Tuple ``(growth_rate_bytes_per_day, days_to_threshold)``:

            * ``(0.0, -1.0)`` when ``len(snapshots) < 3`` (Requirement 12.2)
              or the timestamps have zero variance (regression undefined).
            * ``(rate, -1.0)`` when the computed growth rate is zero or
              negative (Requirement 12.3 — no exhaustion projected).
            * ``(rate, 0.0)`` when ``current_bytes > threshold_bytes``
              (Requirement 12.4 — already exceeded).
            * ``(rate, days)`` with ``days = (threshold - current) / rate``
              otherwise (Requirement 12.5).
        """
        # Requirement 12.2: insufficient history short-circuits. The
        # threshold of 3 is the minimum for a meaningful least-squares
        # slope on real-world noisy data.
        if len(snapshots) < 3:
            return 0.0, _NO_PROJECTION

        # Convert to (days_since_t0, bytes) coordinates. Using
        # ``total_seconds() / 86400`` keeps the X axis in days so the
        # regression slope is directly bytes-per-day.
        t0 = snapshots[0][0]
        xs = [(ts - t0).total_seconds() / 86400.0 for ts, _ in snapshots]
        ys = [float(value) for _, value in snapshots]
        n = len(xs)

        mean_x = sum(xs) / n
        mean_y = sum(ys) / n

        numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        denominator = sum((x - mean_x) ** 2 for x in xs)

        # Degenerate case: all snapshots have the same timestamp (zero
        # variance in X). The regression is mathematically undefined, so
        # report no projection rather than dividing by zero.
        if denominator == 0:
            return 0.0, _NO_PROJECTION

        growth_rate = numerator / denominator

        # Requirement 12.4 takes precedence over 12.3: if we're already
        # over threshold, days-to-threshold is 0 regardless of slope
        # direction (shrinking while over-threshold is still an alert
        # condition — the operator wants to know capacity is exceeded).
        if current_bytes > threshold_bytes:
            return growth_rate, _ALREADY_OVER_THRESHOLD

        # Requirement 12.3: zero or negative growth → no exhaustion.
        if growth_rate <= 0:
            return growth_rate, _NO_PROJECTION

        # Requirement 12.5: linear projection of remaining headroom.
        days_to_threshold = (threshold_bytes - current_bytes) / growth_rate
        return growth_rate, days_to_threshold

    # ------------------------------------------------------------------
    # Collection cycle
    # ------------------------------------------------------------------

    async def collect(self) -> None:
        """Run one capacity-planning cycle.

        Orchestrates size collection across all configured engines,
        persists snapshots, runs the growth regression for PostgreSQL
        (the only engine with a ``growth_rate`` gauge by design §5.9),
        computes per-engine days-to-threshold, updates all
        ``hydra_capacity_*`` gauges, and runs retention cleanup.

        Per-engine failures are caught inside this method so a single
        backend outage cannot suppress metrics for healthy engines
        (Requirement 22.3). Any exception escaping this method is
        caught by :meth:`BaseCollector._loop`.
        """
        # Per-engine size maps. The PG entry is populated first because
        # the subsequent regression, persistence, and cleanup steps all
        # require a working pg_pool. If PG itself fails we still attempt
        # the other engines (for metric exposure) but skip persistence.
        pg_sizes: dict[str, int] = {}
        es_sizes: dict[str, int] = {}
        influx_sizes: dict[str, int] = {}
        minio_sizes: dict[str, int] = {}

        pg_available = False

        # --- PostgreSQL sizes -----------------------------------------
        try:
            pg_sizes = await self._collect_pg_sizes()
            pg_available = True
        except Exception as exc:  # noqa: BLE001 — per-engine isolation
            self._log_engine_error(ENGINE_POSTGRES, exc)

        # --- Elasticsearch sizes --------------------------------------
        if self._es_backend is not None:
            try:
                es_sizes = await self._collect_es_sizes(self._es_backend)
            except Exception as exc:  # noqa: BLE001
                self._log_engine_error(ENGINE_ELASTICSEARCH, exc)
        else:
            logger.debug("CapacityPlanner: no ES backend configured; skipping")

        # --- InfluxDB size --------------------------------------------
        if self._influx_backend is not None:
            try:
                influx_sizes = await self._collect_influx_sizes(
                    self._influx_backend
                )
            except Exception as exc:  # noqa: BLE001
                self._log_engine_error(ENGINE_INFLUXDB, exc)
        else:
            logger.debug("CapacityPlanner: no InfluxDB backend configured; skipping")

        # --- MinIO sizes ----------------------------------------------
        if self._minio_backend is not None:
            try:
                minio_sizes = await self._collect_minio_sizes(self._minio_backend)
            except Exception as exc:  # noqa: BLE001
                self._log_engine_error(ENGINE_MINIO, exc)
        else:
            logger.debug("CapacityPlanner: no MinIO backend configured; skipping")

        # --- Publish size gauges --------------------------------------
        self._publish_size_gauges(
            pg_sizes=pg_sizes,
            es_sizes=es_sizes,
            influx_sizes=influx_sizes,
            minio_sizes=minio_sizes,
        )

        # --- Persistence, regression, cleanup (PG-backed) -------------
        if not pg_available:
            # Without PG we cannot persist snapshots, query history, or
            # run retention cleanup. Size gauges have already been
            # published above, which is still useful for dashboards.
            return

        try:
            async with self._pg_pool.acquire() as conn:
                # 1. Persist this cycle's snapshots.
                await self._persist_snapshots(
                    conn,
                    pg_sizes=pg_sizes,
                    es_sizes=es_sizes,
                    influx_sizes=influx_sizes,
                    minio_sizes=minio_sizes,
                )

                # 2. Run the PG growth regression and update days-to-
                #    threshold. PG is the only engine for which we
                #    expose a growth-rate gauge (design §5.9) — for ES,
                #    InfluxDB, and MinIO we compute days-to-threshold
                #    from their current size and a best-effort
                #    regression but do not publish a growth-rate gauge.
                await self._update_growth_projections(
                    conn,
                    pg_current=pg_sizes.get(_PG_DATABASE_METRIC, 0),
                    es_sizes=es_sizes,
                    influx_current=influx_sizes.get(_INFLUX_METRIC, 0),
                    minio_sizes=minio_sizes,
                )

                # 3. Retention cleanup. Runs inside the same connection
                #    acquisition for efficiency.
                await self._cleanup_old_snapshots(conn)
        except Exception as exc:  # noqa: BLE001 — persistence is a single logical step
            # Any failure here is already after size metrics were
            # published, so we simply log and let the next cycle retry.
            self._log_engine_error(ENGINE_POSTGRES, exc)

    # ------------------------------------------------------------------
    # Per-engine size collectors
    # ------------------------------------------------------------------

    async def _collect_pg_sizes(self) -> dict[str, int]:
        """Return PostgreSQL database + per-table sizes in bytes.

        The returned dict uses the metric-name schema:

        * ``_PG_DATABASE_METRIC`` → total database size
          (``pg_database_size``).
        * ``_PG_TABLE_METRIC_PREFIX + <table>`` → per-table size
          (``pg_total_relation_size``).

        Tables missing from the database (``to_regclass`` returns
        ``NULL``) are silently omitted rather than reported as zero.

        Raises:
            asyncpg.PostgresError: On connection failure. Caller (the
                :meth:`collect` cycle) catches and logs.
        """
        sizes: dict[str, int] = {}
        async with self._pg_pool.acquire() as conn:
            db_row = await conn.fetchrow(_PG_DATABASE_SIZE_SQL)
            if db_row is not None and db_row["size_bytes"] is not None:
                sizes[_PG_DATABASE_METRIC] = int(db_row["size_bytes"])

            for table in _PG_KEY_TABLES:
                row = await conn.fetchrow(_PG_TABLE_SIZE_SQL, table)
                # ``to_regclass`` returns NULL for unknown tables and
                # ``pg_total_relation_size(NULL)`` returns NULL —
                # skip those rather than misreport zero bytes.
                if row is None or row["size_bytes"] is None:
                    continue
                sizes[f"{_PG_TABLE_METRIC_PREFIX}{table}"] = int(row["size_bytes"])
        return sizes

    async def _collect_es_sizes(
        self, es_backend: ESSizeBackend
    ) -> dict[str, int]:
        """Return per-index Elasticsearch sizes keyed by ``es_index:<index>``."""
        raw = await es_backend.fetch_index_sizes()
        return {f"{_ES_METRIC_PREFIX}{name}": int(size) for name, size in raw.items()}

    async def _collect_influx_sizes(
        self, influx_backend: InfluxSizeBackend
    ) -> dict[str, int]:
        """Return aggregate InfluxDB bucket size keyed by ``_INFLUX_METRIC``."""
        size = await influx_backend.fetch_bucket_size()
        return {_INFLUX_METRIC: int(size)}

    async def _collect_minio_sizes(
        self, minio_backend: MinIOSizeBackend
    ) -> dict[str, int]:
        """Return per-bucket MinIO sizes keyed by ``minio_bucket:<bucket>``."""
        raw = await minio_backend.fetch_bucket_sizes()
        return {
            f"{_MINIO_METRIC_PREFIX}{bucket}": int(size)
            for bucket, size in raw.items()
        }

    # ------------------------------------------------------------------
    # Persistence, regression, cleanup
    # ------------------------------------------------------------------

    async def _persist_snapshots(
        self,
        conn: "asyncpg.Connection",
        *,
        pg_sizes: dict[str, int],
        es_sizes: dict[str, int],
        influx_sizes: dict[str, int],
        minio_sizes: dict[str, int],
    ) -> None:
        """INSERT one row per observed size into ``capacity_snapshots``.

        ``collected_at`` defaults to ``NOW()`` in the schema, so we let
        the database assign it rather than sending a Python timestamp —
        this keeps all snapshots on the database clock and avoids skew
        if the API host and DB host disagree.

        Requirement 13.5.
        """
        # Compose (engine, metric_name, value_bytes) tuples. Engines
        # whose collection failed earlier have empty dicts here and
        # contribute no rows.
        rows: list[tuple[str, str, int]] = []
        for metric_name, value in pg_sizes.items():
            rows.append((ENGINE_POSTGRES, metric_name, value))
        for metric_name, value in es_sizes.items():
            rows.append((ENGINE_ELASTICSEARCH, metric_name, value))
        for metric_name, value in influx_sizes.items():
            rows.append((ENGINE_INFLUXDB, metric_name, value))
        for metric_name, value in minio_sizes.items():
            rows.append((ENGINE_MINIO, metric_name, value))

        if not rows:
            return

        # Use ``executemany`` so all inserts go through one round trip
        # per batch rather than one per row.
        await conn.executemany(_INSERT_SNAPSHOT_SQL, rows)

    async def _update_growth_projections(
        self,
        conn: "asyncpg.Connection",
        *,
        pg_current: int,
        es_sizes: dict[str, int],
        influx_current: int,
        minio_sizes: dict[str, int],
    ) -> None:
        """Compute growth rate + days-to-threshold for each engine.

        PostgreSQL gets both a growth-rate gauge and a days-to-threshold
        gauge. The other engines get a days-to-threshold gauge only —
        their growth rates can be derived from the raw snapshot history
        in Grafana via PromQL ``deriv()`` or direct SQL if needed.

        Requirements 12.1–12.5, 13.7.
        """
        # PostgreSQL — headline database size drives the growth gauge.
        if pg_current > 0:
            pg_history = await self._fetch_history(
                conn, ENGINE_POSTGRES, _PG_DATABASE_METRIC
            )
            pg_rate, pg_days = self._project_growth(
                pg_history,
                self._settings.capacity_pg_threshold_bytes,
                pg_current,
            )
            hydra_capacity_pg_growth_rate_bytes_per_day.set(pg_rate)
            hydra_capacity_days_to_threshold.labels(
                engine=ENGINE_POSTGRES
            ).set(pg_days)

        # Elasticsearch — aggregate index sizes into a single engine
        # total for threshold comparison. Individual index sizes are
        # already surfaced via their own gauge.
        if es_sizes:
            es_total = sum(es_sizes.values())
            # Regress on the aggregated total rather than each index —
            # operators care about "when does ES run out of disk", not
            # per-index projections.
            es_history = await self._fetch_history_aggregated(
                conn, ENGINE_ELASTICSEARCH
            )
            _, es_days = self._project_growth(
                es_history,
                self._settings.capacity_es_threshold_bytes,
                es_total,
            )
            hydra_capacity_days_to_threshold.labels(
                engine=ENGINE_ELASTICSEARCH
            ).set(es_days)

        # InfluxDB — single-bucket metric, straightforward regression.
        if influx_current > 0:
            influx_history = await self._fetch_history(
                conn, ENGINE_INFLUXDB, _INFLUX_METRIC
            )
            _, influx_days = self._project_growth(
                influx_history,
                self._settings.capacity_influx_threshold_bytes,
                influx_current,
            )
            hydra_capacity_days_to_threshold.labels(
                engine=ENGINE_INFLUXDB
            ).set(influx_days)

        # MinIO — aggregate like ES.
        if minio_sizes:
            minio_total = sum(minio_sizes.values())
            minio_history = await self._fetch_history_aggregated(
                conn, ENGINE_MINIO
            )
            _, minio_days = self._project_growth(
                minio_history,
                self._settings.capacity_minio_threshold_bytes,
                minio_total,
            )
            hydra_capacity_days_to_threshold.labels(
                engine=ENGINE_MINIO
            ).set(minio_days)

    async def _fetch_history(
        self,
        conn: "asyncpg.Connection",
        engine: str,
        metric_name: str,
    ) -> list[tuple[datetime, int]]:
        """Return ``(collected_at, value_bytes)`` history for one metric.

        Bounded by the 7-day regression window (design §"Algorithm 4",
        Requirement 12.1). Rows are sorted ascending by ``collected_at``
        so callers can feed them directly to :meth:`_project_growth`.
        """
        rows = await conn.fetch(
            _SELECT_PG_HISTORY_SQL, engine, metric_name, _REGRESSION_WINDOW_DAYS
        )
        return [(row["collected_at"], int(row["value_bytes"])) for row in rows]

    async def _fetch_history_aggregated(
        self,
        conn: "asyncpg.Connection",
        engine: str,
    ) -> list[tuple[datetime, int]]:
        """Return per-timestamp summed history for a multi-metric engine.

        For engines that store multiple snapshot rows per cycle (ES,
        MinIO — one row per index/bucket), collapse into a single series
        by summing all rows that share the same ``collected_at``. The
        resulting series is what ``_project_growth`` expects.
        """
        query = (
            "SELECT collected_at, SUM(value_bytes)::bigint AS total_bytes "
            "FROM capacity_snapshots "
            "WHERE engine = $1 "
            "  AND collected_at > NOW() - ($2::int * INTERVAL '1 day') "
            "GROUP BY collected_at "
            "ORDER BY collected_at ASC"
        )
        rows = await conn.fetch(query, engine, _REGRESSION_WINDOW_DAYS)
        return [(row["collected_at"], int(row["total_bytes"])) for row in rows]

    async def _cleanup_old_snapshots(self, conn: "asyncpg.Connection") -> None:
        """Delete snapshots older than ``capacity_history_retention_days``.

        Uses parameter binding to avoid string interpolation
        (Requirement 13.6, safety guardrail on SQL injection).
        """
        await conn.execute(
            _CLEANUP_SQL, self._settings.capacity_history_retention_days
        )

    # ------------------------------------------------------------------
    # Metric publication helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _publish_size_gauges(
        *,
        pg_sizes: dict[str, int],
        es_sizes: dict[str, int],
        influx_sizes: dict[str, int],
        minio_sizes: dict[str, int],
    ) -> None:
        """Update every ``hydra_capacity_*_size_bytes`` gauge.

        Called after all collection attempts (successful or not) so that
        healthy engines publish even when others are failing. Empty dicts
        simply produce no gauge updates (Requirement 22.3).
        """
        # PostgreSQL — headline gauge + per-table gauge.
        if _PG_DATABASE_METRIC in pg_sizes:
            hydra_capacity_pg_size_bytes.set(pg_sizes[_PG_DATABASE_METRIC])
        for metric_name, value in pg_sizes.items():
            if metric_name.startswith(_PG_TABLE_METRIC_PREFIX):
                table = metric_name[len(_PG_TABLE_METRIC_PREFIX) :]
                hydra_capacity_pg_table_size_bytes.labels(table=table).set(value)

        # Elasticsearch — one gauge per index label.
        for metric_name, value in es_sizes.items():
            index = metric_name[len(_ES_METRIC_PREFIX) :]
            hydra_capacity_es_index_size_bytes.labels(index=index).set(value)

        # InfluxDB — single aggregate gauge.
        if _INFLUX_METRIC in influx_sizes:
            hydra_capacity_influx_bucket_size_bytes.set(
                influx_sizes[_INFLUX_METRIC]
            )

        # MinIO — one gauge per bucket label.
        for metric_name, value in minio_sizes.items():
            bucket = metric_name[len(_MINIO_METRIC_PREFIX) :]
            hydra_capacity_minio_bucket_size_bytes.labels(bucket=bucket).set(value)

    @staticmethod
    def _log_engine_error(engine: str, exc: BaseException) -> None:
        """Log a per-engine failure as a :class:`CapacityPlanningError`.

        The original exception is preserved via ``__cause__`` (implicit
        when caught and re-raised with ``from``) but we don't actually
        re-raise — the cycle must continue with the other engines
        (Requirement 22.3). The wrapped ``CapacityPlanningError`` is
        logged with ``exc_info`` so the stack trace is captured.
        """
        err = CapacityPlanningError(
            f"Capacity collection failed for engine={engine!r}: {exc}"
        )
        err.__cause__ = exc
        logger.error(
            "CapacityPlanner engine=%s failed: %s", engine, exc, exc_info=err
        )


__all__ = [
    "ENGINE_POSTGRES",
    "ENGINE_ELASTICSEARCH",
    "ENGINE_INFLUXDB",
    "ENGINE_MINIO",
    "ESSizeBackend",
    "InfluxSizeBackend",
    "MinIOSizeBackend",
    "CapacityPlanner",
]
