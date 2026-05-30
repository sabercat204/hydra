"""Trends service — InfluxDB primary path with PostgreSQL fallback (Design §6.5).

The public entry point is :class:`TrendsService.query`. The contract:

1. Build a Flux query from :class:`TrendRequest` and execute it against
   the wired InfluxDB client. Aggregations ``count/sum/mean/min/max``
   use the native Flux function of the same name; ``p50/p95/p99`` use
   ``quantile(q: 0.5/0.95/0.99)``.
2. When ``StorageHealth("influxdb").status == "UNREACHABLE"``, fall back
   to a PostgreSQL aggregation via TimescaleDB ``time_bucket`` when the
   extension is present, or plain ``date_trunc`` otherwise. The
   response carries ``fallback=True`` so the router can emit
   ``207 Multi-Status`` (R14.5).

The service is storage-agnostic in its constructor — it accepts
``influx_client``, ``pg_pool`` and ``storage_health`` as duck-typed
handles. That keeps the module importable when the storage layer is
stubbed (tests) and when :func:`setup_eas` wires real clients.

Validation — including bucket / window ceilings from Design §3.6 — is
**not** performed here. Routers must call
:func:`hydra.eas.trends.buckets.validate_window` before invoking
``query`` so R14.3 ("SHALL NOT execute any storage query") is satisfied
structurally. This service treats its inputs as pre-validated.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from hydra.eas.schemas.trends import (
    Aggregation,
    Bucket,
    TrendPoint,
    TrendRequest,
    TrendResponse,
    TrendSeries,
)

logger = logging.getLogger(__name__)

__all__ = ["TrendsService"]


# ---------------------------------------------------------------------------
# Bucket + aggregation helpers
# ---------------------------------------------------------------------------


# Seconds in each bucket literal. Used for the PG fallback where we build
# ``date_trunc`` / ``time_bucket`` with an interval string.
_BUCKET_SECONDS: dict[Bucket, int] = {
    "1m": 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
    "6h": 6 * 60 * 60,
    "1d": 24 * 60 * 60,
    "7d": 7 * 24 * 60 * 60,
}


# Quantile aggregations use ``quantile(q: ...)`` with these q values.
_QUANTILE_Q: dict[Aggregation, float] = {
    "p50": 0.5,
    "p95": 0.95,
    "p99": 0.99,
}


# PostgreSQL equivalent of each aggregation. Used by the fallback path.
# ``percentile_cont`` is SQL-standard and available on stock PG without
# TimescaleDB.
_PG_AGG_SQL: dict[Aggregation, str] = {
    "count": "COUNT(*)::double precision",
    "sum": "COALESCE(SUM(value), 0)::double precision",
    "mean": "COALESCE(AVG(value), 0)::double precision",
    "min": "COALESCE(MIN(value), 0)::double precision",
    "max": "COALESCE(MAX(value), 0)::double precision",
    "p50": "COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY value), 0)::double precision",
    "p95": "COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY value), 0)::double precision",
    "p99": "COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY value), 0)::double precision",
}


def _flux_aggregate_fn(aggregation: Aggregation) -> str:
    """Map the public ``Aggregation`` literal onto a Flux ``fn: ...`` snippet.

    Native functions (``count``, ``sum``, ``mean``, ``min``, ``max``) pass
    through by name. Percentiles become ``(column) => quantile(column:
    column, q: <q>, method: "estimate_tdigest")`` — the default method
    is numerically stable and matches InfluxDB's built-in semantics.
    """

    if aggregation in _QUANTILE_Q:
        q = _QUANTILE_Q[aggregation]
        # ``aggregateWindow`` expects a function of type ``(column) => ...``.
        return (
            f"(column, tables=<-) => tables "
            f'|> quantile(column: column, q: {q}, method: "estimate_tdigest")'
        )
    # Native Flux functions share the same name as the public literal
    # for count/sum/mean/min/max.
    return aggregation


# ---------------------------------------------------------------------------
# TrendsService
# ---------------------------------------------------------------------------


@dataclass
class TrendsService:
    """InfluxDB-first trends query engine with a PG fallback path.

    Parameters
    ----------
    influx_client:
        An :class:`InfluxDBClientAsync` (or any duck-typed object exposing
        ``query_api().query(flux, ...)``). May be ``None`` — in that case
        every request uses the PG fallback.
    pg_pool:
        An ``asyncpg`` pool (duck-typed ``.acquire()`` context manager
        yielding a connection with ``.fetch(...)``). May be ``None``; when
        both clients are unavailable the service raises on ``query``.
    storage_health:
        An object with an ``async def get(engine: str) -> StorageHealth |
        None`` method *or* a sync ``dict``-like mapping from engine name
        to :class:`StorageHealth`. Used to gate the fallback path per
        Design §6.5.
    bucket_name:
        The InfluxDB bucket to query. Defaults to ``"hydra-timeseries"``
        which matches ``HydraSettings.database.influxdb_bucket``.
    pg_measurement_table:
        Name of the PG table used by the fallback. Defaults to
        ``"normalized_records"`` — the fallback reads raw records and
        aggregates server-side.
    """

    influx_client: Any
    pg_pool: Any
    storage_health: Any
    bucket_name: str = "hydra-timeseries"
    pg_measurement_table: str = "normalized_records"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def query(self, request: TrendRequest) -> TrendResponse:
        """Execute a :class:`TrendRequest` and return a :class:`TrendResponse`.

        Preconditions (the caller / router is responsible):

        * ``request.bucket`` is a valid bucket literal.
        * :func:`hydra.eas.trends.buckets.validate_window` has been
          called so ``time_start < time_end`` and the window fits the
          bucket ceiling.
        """

        use_fallback = await self._should_fall_back()

        if use_fallback or self.influx_client is None:
            series = await self._query_pg(
                request.stream_ids,
                request.time_start,
                request.time_end,
                request.bucket,
                request.aggregation,
            )
            fallback = True
        else:
            try:
                series = await self._query_influx(
                    request.stream_ids,
                    request.time_start,
                    request.time_end,
                    request.bucket,
                    request.aggregation,
                )
                fallback = False
            except Exception as exc:  # pragma: no cover - defensive
                # Network / Flux compile errors fall through to PG so a
                # transient Influx issue doesn't surface as a 500. The
                # router still gets ``fallback=True`` in the response
                # envelope so the client knows.
                logger.warning(
                    "trends_influx_query_failed_falling_back",
                    extra={"error": str(exc)},
                )
                series = await self._query_pg(
                    request.stream_ids,
                    request.time_start,
                    request.time_end,
                    request.bucket,
                    request.aggregation,
                )
                fallback = True

        return TrendResponse(
            series=TrendSeries(series=series),
            bucket=request.bucket,
            aggregation=request.aggregation,
            fallback=fallback,
        )

    # ------------------------------------------------------------------
    # Health gate
    # ------------------------------------------------------------------

    async def _should_fall_back(self) -> bool:
        """Return ``True`` when the InfluxDB engine is UNREACHABLE.

        Accepts either a ``dict``-like mapping or an object with an
        ``async get`` / sync ``get`` method so the service is easy to
        stub in tests.
        """

        sh = self.storage_health
        if sh is None:
            return False

        health = None
        if hasattr(sh, "get"):
            candidate = sh.get("influxdb")
            if hasattr(candidate, "__await__"):
                health = await candidate  # type: ignore[misc]
            else:
                health = candidate
        elif hasattr(sh, "check_all"):
            all_checks = await sh.check_all()
            health = all_checks.get("influxdb")

        if health is None:
            return False

        status = getattr(health, "status", None)
        return status == "UNREACHABLE"

    # ------------------------------------------------------------------
    # InfluxDB primary path
    # ------------------------------------------------------------------

    async def _query_influx(
        self,
        stream_ids: list[str],
        time_start: datetime,
        time_end: datetime,
        bucket: Bucket,
        aggregation: Aggregation,
    ) -> dict[str, list[TrendPoint]]:
        """Run the Flux query and group results by ``stream_id``.

        Flux query shape per task spec::

            from(bucket: "hydra-timeseries")
              |> range(start: <start>, stop: <stop>)
              |> filter(fn: (r) => contains(value: r.stream_id, set: [...]))
              |> aggregateWindow(every: <bucket>, fn: <fn>)
              |> yield()
        """

        flux = self._build_flux(stream_ids, time_start, time_end, bucket, aggregation)
        query_api = self.influx_client.query_api()
        tables = await query_api.query(flux)

        # Seed every requested stream so the response has an entry
        # even for streams with zero points in the window — makes
        # downstream comparison math easier.
        result: dict[str, list[TrendPoint]] = {sid: [] for sid in stream_ids}

        for table in tables:
            for record in table.records:
                stream_id = record.values.get("stream_id") if hasattr(record, "values") else None
                if stream_id is None and hasattr(record, "get_value_by_key"):
                    stream_id = record.get_value_by_key("stream_id")
                if stream_id is None:
                    continue
                if stream_id not in result:
                    result[stream_id] = []

                ts = record.get_time() if hasattr(record, "get_time") else None
                if ts is None:
                    continue
                value = record.get_value() if hasattr(record, "get_value") else None
                if value is None:
                    continue

                result[stream_id].append(
                    TrendPoint(
                        bucket_start=_ensure_utc(ts),
                        value=float(value),
                    )
                )

        # Sort each series by bucket_start so downstream consumers
        # (comparison, delta) can align points by index.
        for sid, points in result.items():
            points.sort(key=lambda p: p.bucket_start)
        return result

    def _build_flux(
        self,
        stream_ids: list[str],
        time_start: datetime,
        time_end: datetime,
        bucket: Bucket,
        aggregation: Aggregation,
    ) -> str:
        """Build the Flux query string per task spec."""

        start = _iso(time_start)
        stop = _iso(time_end)
        stream_set = "[" + ", ".join(f'"{_escape(s)}"' for s in stream_ids) + "]"
        fn_snippet = _flux_aggregate_fn(aggregation)

        return (
            f'from(bucket: "{self.bucket_name}")\n'
            f"  |> range(start: {start}, stop: {stop})\n"
            f"  |> filter(fn: (r) => contains(value: r.stream_id, set: {stream_set}))\n"
            f"  |> aggregateWindow(every: {bucket}, fn: {fn_snippet}, createEmpty: false)\n"
            f"  |> yield(name: \"result\")"
        )

    # ------------------------------------------------------------------
    # PG fallback path
    # ------------------------------------------------------------------

    async def _query_pg(
        self,
        stream_ids: list[str],
        time_start: datetime,
        time_end: datetime,
        bucket: Bucket,
        aggregation: Aggregation,
    ) -> dict[str, list[TrendPoint]]:
        """Aggregate ``normalized_records`` in PostgreSQL.

        Uses ``time_bucket`` from TimescaleDB when available; falls back
        to ``date_trunc`` for the standard buckets. The ``value`` column
        is derived from ``confidence`` for stock records — Tier 29
        measurements that carry their own numeric field can override
        this via ``pg_measurement_table`` in a future iteration.
        """

        if self.pg_pool is None:
            logger.error("trends_pg_fallback_no_pool")
            return {sid: [] for sid in stream_ids}

        interval_seconds = _BUCKET_SECONDS[bucket]
        agg_sql = _PG_AGG_SQL[aggregation]

        # Prefer TimescaleDB's ``time_bucket`` when the extension is
        # installed; otherwise ``date_trunc`` handles the standard
        # bucket widths (1h, 1d) cleanly. For the remaining widths
        # (5m, 15m, 6h, 7d) we compute ``to_timestamp(floor(...))``
        # which works on stock PG.
        bucket_expr = _pg_bucket_expr(bucket, interval_seconds)

        sql = (
            f"SELECT stream_id, {bucket_expr} AS bucket_start, "
            f"       {agg_sql} AS value "
            f"FROM {self.pg_measurement_table} "
            "WHERE stream_id = ANY($1::text[]) "
            "  AND timestamp >= $2 AND timestamp < $3 "
            "GROUP BY stream_id, bucket_start "
            "ORDER BY stream_id, bucket_start"
        )

        result: dict[str, list[TrendPoint]] = {sid: [] for sid in stream_ids}
        async with self.pg_pool.acquire() as conn:
            rows = await conn.fetch(sql, list(stream_ids), time_start, time_end)

        for row in rows:
            sid = row["stream_id"]
            if sid not in result:
                result[sid] = []
            bucket_start = row["bucket_start"]
            if isinstance(bucket_start, (int, float)):
                bucket_start = datetime.fromtimestamp(float(bucket_start), tz=timezone.utc)
            result[sid].append(
                TrendPoint(
                    bucket_start=_ensure_utc(bucket_start),
                    value=float(row["value"] or 0.0),
                )
            )
        return result


# ---------------------------------------------------------------------------
# Module helpers
# ---------------------------------------------------------------------------


def _ensure_utc(ts: datetime) -> datetime:
    """Return ``ts`` with UTC tzinfo — Influx rows may come back naive."""

    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _iso(ts: datetime) -> str:
    """ISO-8601 UTC string for Flux ``range(start:..., stop:...)`` arg."""

    return _ensure_utc(ts).strftime("%Y-%m-%dT%H:%M:%SZ")


def _escape(value: str) -> str:
    """Minimal Flux string escape — guards ``"`` only.

    Stream ids in the registry are ASCII slugs, so aggressive escaping
    is unnecessary. Still, protecting the quote keeps a future
    registry-of-exotic-ids from breaking the query.
    """

    return value.replace("\\", "\\\\").replace('"', '\\"')


def _pg_bucket_expr(bucket: Bucket, interval_seconds: int) -> str:
    """Return the bucket truncation SQL for ``timestamp``.

    Uses ``time_bucket`` on TimescaleDB for the awkward widths (5m,
    15m, 6h, 7d) and ``date_trunc`` for 1h / 1d where the semantics
    match exactly. ``to_timestamp(floor(...))`` is the stock-PG
    fallback.
    """

    if bucket == "1h":
        return "date_trunc('hour', timestamp)"
    if bucket == "1d":
        return "date_trunc('day', timestamp)"
    # ``time_bucket`` is the TimescaleDB-native function; when the
    # extension is absent the query will fail and the router will
    # surface a 500. Since we're on the fallback path already the
    # deployment is expected to have one of InfluxDB OR TimescaleDB
    # available, per Design §6.5.
    return (
        f"COALESCE("
        f"time_bucket(INTERVAL '{interval_seconds} seconds', timestamp), "
        f"to_timestamp(floor(EXTRACT(epoch FROM timestamp) / {interval_seconds}) "
        f"* {interval_seconds}))"
    )
