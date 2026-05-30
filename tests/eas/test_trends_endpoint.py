"""Integration tests for the trends service (task 11.10).

Exercises :class:`TrendsService` end-to-end with both the primary
Influx path and the PostgreSQL fallback. The router layer is a thin
``Depends``-driven wrapper over ``service.query``, so driving the
service directly is the right level to assert:

* Primary Influx path returns ``fallback=False``.
* ``StorageHealth("influxdb") == UNREACHABLE`` causes PG fallback with
  ``fallback=True``.
* Window-validation errors propagate from
  :func:`hydra.eas.trends.buckets.validate_window` before any storage
  query fires (R14.3 / Property 16).
* ``compare_to="previous_period"`` produces a comparison + delta
  series (R14.4).
* Influx transient errors trigger automatic PG fallback.

Validates: R14.1, R14.2, R14.3, R14.4, R14.5, Property 16.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from hydra.api.errors import ErrorCode, HydraAPIException
from hydra.eas.schemas.trends import TrendRequest
from hydra.eas.trends.buckets import validate_window
from hydra.eas.trends.comparison import compute_comparison
from hydra.eas.trends.service import TrendsService


# ---------------------------------------------------------------------------
# FakeInfluxClient — emulates the 8.x async Influx query API surface
# ---------------------------------------------------------------------------


@dataclass
class _FakeRecord:
    """Single row returned by the fake Influx query.

    ``values`` is a dict keyed by column name; the service reads
    ``stream_id`` from it and calls ``get_time()`` / ``get_value()``
    for the timestamp and numeric value. We implement the three
    accessors the service uses verbatim.
    """

    stream_id: str
    timestamp: datetime
    value: float

    @property
    def values(self) -> dict[str, Any]:
        return {"stream_id": self.stream_id}

    def get_time(self) -> datetime:
        return self.timestamp

    def get_value(self) -> float:
        return self.value


@dataclass
class _FakeTable:
    """An Influx query response is a list of tables; each has records."""

    records: list[_FakeRecord]


class _FakeInfluxQueryAPI:
    def __init__(self, tables: list[_FakeTable], *, raises: Exception | None = None) -> None:
        self._tables = tables
        self._raises = raises
        self.calls: list[str] = []

    async def query(self, flux: str) -> list[_FakeTable]:
        self.calls.append(flux)
        if self._raises is not None:
            raise self._raises
        return self._tables


class FakeInfluxClient:
    """Shape-compatible Influx double — the service calls ``query_api().query(flux)``."""

    def __init__(
        self,
        tables: list[_FakeTable] | None = None,
        raises: Exception | None = None,
    ) -> None:
        self._api = _FakeInfluxQueryAPI(tables or [], raises=raises)

    def query_api(self) -> _FakeInfluxQueryAPI:
        return self._api

    @property
    def calls(self) -> list[str]:
        return self._api.calls


# ---------------------------------------------------------------------------
# FakePgPool — drives the PG fallback path
# ---------------------------------------------------------------------------


class _FakePgConn:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        return list(self._rows)


class FakePgPool:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = rows or []

    def acquire(self) -> "FakePgPool":
        return self

    async def __aenter__(self) -> _FakePgConn:
        return _FakePgConn(self._rows)

    async def __aexit__(self, *exc: Any) -> None:
        return None


# ---------------------------------------------------------------------------
# FakeStorageHealth — exposes ``.get(engine)`` returning an object with ``.status``
# ---------------------------------------------------------------------------


class _HealthResult:
    def __init__(self, status: str) -> None:
        self.status = status


class FakeStorageHealth:
    """Dict-like stand-in for the storage health probe the service reads.

    The service calls ``health.get(engine)`` and branches on the
    returned object's ``.status`` attribute. We return ``None`` for
    engines we haven't seeded so the service treats them as healthy.
    """

    def __init__(self, statuses: dict[str, str] | None = None) -> None:
        self._statuses = statuses or {}

    def get(self, engine: str) -> _HealthResult | None:
        status = self._statuses.get(engine)
        return _HealthResult(status) if status else None


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


_EPOCH = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _request(
    *,
    stream_ids: list[str] | None = None,
    time_start: datetime | None = None,
    time_end: datetime | None = None,
    bucket: str = "1h",
    aggregation: str = "count",
    compare_to: str | None = None,
) -> TrendRequest:
    return TrendRequest(
        stream_ids=stream_ids or ["stream-a"],
        time_start=time_start or _EPOCH,
        time_end=time_end or (_EPOCH + timedelta(hours=3)),
        bucket=bucket,  # type: ignore[arg-type]
        aggregation=aggregation,  # type: ignore[arg-type]
        compare_to=compare_to,  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Primary Influx path
# ---------------------------------------------------------------------------


async def test_primary_influx_path_returns_fallback_false() -> None:
    """Happy path — Influx responds; service returns ``fallback=False``.

    Also verifies the Flux query carries the stream ids the caller
    requested so a regression in the query builder is caught.
    """

    tables = [_FakeTable(records=[
        _FakeRecord("stream-a", _EPOCH, 10.0),
        _FakeRecord("stream-a", _EPOCH + timedelta(hours=1), 20.0),
    ])]
    influx = FakeInfluxClient(tables=tables)
    pg = FakePgPool()
    health = FakeStorageHealth()

    service = TrendsService(
        influx_client=influx, pg_pool=pg, storage_health=health
    )

    response = await service.query(_request())

    assert response.fallback is False
    assert response.bucket == "1h"
    assert response.aggregation == "count"
    # Stream series populated from the fake Influx rows.
    series = response.series.series
    assert "stream-a" in series
    assert len(series["stream-a"]) == 2
    # Flux carried our stream id.
    assert '"stream-a"' in influx.calls[0]


async def test_primary_influx_path_sorts_points_by_bucket_start() -> None:
    """Service sorts each stream's points by ``bucket_start`` ascending.

    Downstream consumers (comparison, delta) depend on a stable sort
    order so two parallel streams align by index.
    """

    # Seed records out of chronological order.
    tables = [_FakeTable(records=[
        _FakeRecord("stream-a", _EPOCH + timedelta(hours=2), 30.0),
        _FakeRecord("stream-a", _EPOCH, 10.0),
        _FakeRecord("stream-a", _EPOCH + timedelta(hours=1), 20.0),
    ])]
    influx = FakeInfluxClient(tables=tables)
    service = TrendsService(influx, FakePgPool(), FakeStorageHealth())

    response = await service.query(_request())
    points = response.series.series["stream-a"]
    timestamps = [p.bucket_start for p in points]
    assert timestamps == sorted(timestamps)


# ---------------------------------------------------------------------------
# PG fallback path — R14.5
# ---------------------------------------------------------------------------


async def test_fallback_when_influx_is_unreachable() -> None:
    """``StorageHealth("influxdb") == UNREACHABLE`` flips the service to PG.

    The response carries ``fallback=True`` so the router can emit a
    ``207 Multi-Status`` envelope per R14.5.
    """

    pg_rows = [
        {"stream_id": "stream-a", "bucket_start": _EPOCH, "value": 42.0},
    ]
    health = FakeStorageHealth({"influxdb": "UNREACHABLE"})
    influx = FakeInfluxClient()  # irrelevant — health gate prevents use
    pg = FakePgPool(pg_rows)

    service = TrendsService(influx, pg, health)

    response = await service.query(_request())

    assert response.fallback is True
    assert response.series.series["stream-a"][0].value == pytest.approx(42.0)
    # Primary path must NOT have been invoked.
    assert influx.calls == []


async def test_fallback_returns_empty_when_pg_unwired() -> None:
    """Fallback without PG returns an empty series but no exception.

    Protects against a misconfigured deployment where Influx is
    unreachable AND PG isn't configured for the service — we'd
    rather return an empty series with ``fallback=True`` than 500.
    """

    health = FakeStorageHealth({"influxdb": "UNREACHABLE"})
    service = TrendsService(
        influx_client=None, pg_pool=None, storage_health=health
    )

    response = await service.query(_request())
    assert response.fallback is True
    assert response.series.series == {"stream-a": []}


async def test_transient_influx_error_flips_to_fallback() -> None:
    """An exception from Influx at query time triggers PG fallback.

    The service's try/except around ``_query_influx`` rescues and
    retries via PG so a transient outage doesn't surface as 500.
    """

    pg_rows = [
        {"stream_id": "stream-a", "bucket_start": _EPOCH, "value": 7.0},
    ]
    influx = FakeInfluxClient(raises=RuntimeError("flux compile error"))
    pg = FakePgPool(pg_rows)
    health = FakeStorageHealth()  # no engine marked unreachable

    service = TrendsService(influx, pg, health)
    response = await service.query(_request())

    assert response.fallback is True
    assert response.series.series["stream-a"][0].value == pytest.approx(7.0)


# ---------------------------------------------------------------------------
# Window validation — R14.2, R14.3, Property 16
# ---------------------------------------------------------------------------


def test_validate_window_raises_on_reverse_window() -> None:
    """R14.2 — ``time_start >= time_end`` raises ``INVALID_TIME_WINDOW``."""

    with pytest.raises(HydraAPIException) as exc:
        validate_window(
            bucket="1h",
            time_start=_EPOCH + timedelta(hours=1),
            time_end=_EPOCH,
            trends_max_window_days=365,
        )
    assert exc.value.code is ErrorCode.INVALID_TIME_WINDOW
    assert exc.value.status_code == 422


def test_validate_window_raises_on_bucket_ceiling_exceeded() -> None:
    """Property 16 / R14.3 — 1m bucket over 30d window is rejected."""

    with pytest.raises(HydraAPIException) as exc:
        validate_window(
            bucket="1m",
            time_start=_EPOCH,
            time_end=_EPOCH + timedelta(days=30),
            trends_max_window_days=365,
        )
    assert exc.value.code is ErrorCode.WINDOW_TOO_LARGE
    assert exc.value.status_code == 422


def test_validate_window_raises_on_global_cap_exceeded() -> None:
    """R14.3 — a global cap below the bucket ceiling takes precedence."""

    with pytest.raises(HydraAPIException) as exc:
        validate_window(
            bucket="1h",
            time_start=_EPOCH,
            time_end=_EPOCH + timedelta(days=90),
            trends_max_window_days=30,
        )
    assert exc.value.code is ErrorCode.WINDOW_TOO_LARGE


def test_validate_window_accepts_at_ceiling() -> None:
    """A window exactly equal to the bucket ceiling is accepted."""

    # 1h bucket ceiling is 365 days per Design §3.6.
    validate_window(
        bucket="1h",
        time_start=_EPOCH,
        time_end=_EPOCH + timedelta(days=365),
        trends_max_window_days=365,
    )
    # No exception == pass.


# ---------------------------------------------------------------------------
# compare_to="previous_period" — R14.4
# ---------------------------------------------------------------------------


async def test_compare_to_previous_period_shape() -> None:
    """R14.4 — a comparison request produces ``series``, ``comparison``, and ``delta``.

    Sets up a deterministic Influx that returns different values
    based on the window requested — the service queries twice, once
    for the current window and once for the previous period. Using
    the ``compute_comparison`` helper directly mirrors what the
    router does.
    """

    # Current window — three points at values 10, 20, 30.
    # Previous period — three points at values 2, 4, 6.
    # Expected delta — [8, 16, 24].
    window_length = timedelta(hours=3)
    t0 = _EPOCH
    t_prev = _EPOCH - window_length

    class _TwoWindowInflux:
        def __init__(self) -> None:
            self._api = self

        def query_api(self):
            return self

        async def query(self, flux: str) -> list[_FakeTable]:
            # Dispatch based on the ``range(start: ...)`` token.
            prev_start = t_prev.strftime("%Y-%m-%dT%H:%M:%SZ")
            if f"range(start: {prev_start}" in flux:
                return [_FakeTable(records=[
                    _FakeRecord("stream-a", t_prev, 2.0),
                    _FakeRecord("stream-a", t_prev + timedelta(hours=1), 4.0),
                    _FakeRecord("stream-a", t_prev + timedelta(hours=2), 6.0),
                ])]
            return [_FakeTable(records=[
                _FakeRecord("stream-a", t0, 10.0),
                _FakeRecord("stream-a", t0 + timedelta(hours=1), 20.0),
                _FakeRecord("stream-a", t0 + timedelta(hours=2), 30.0),
            ])]

    influx = _TwoWindowInflux()
    pg = FakePgPool()
    health = FakeStorageHealth()

    service = TrendsService(
        influx_client=influx,  # type: ignore[arg-type]
        pg_pool=pg,
        storage_health=health,
    )

    request = _request(
        time_start=t0,
        time_end=t0 + window_length,
        compare_to="previous_period",
    )

    trends = await compute_comparison(service, request)

    current = trends.series["stream-a"]
    comparison = (trends.comparison or {})["stream-a"]
    delta = (trends.delta or {})["stream-a"]

    assert [p.value for p in current] == [10.0, 20.0, 30.0]
    assert [p.value for p in comparison] == [2.0, 4.0, 6.0]
    assert [p.value for p in delta] == [8.0, 16.0, 24.0]


# ---------------------------------------------------------------------------
# Stream-id seeding — every requested stream appears even with zero rows
# ---------------------------------------------------------------------------


async def test_service_seeds_requested_stream_ids_even_when_empty() -> None:
    """Every ``stream_ids`` entry shows up in the response, possibly empty.

    Downstream consumers (comparison, delta) rely on this so they can
    pair series up by key without worrying about missing entries.
    """

    influx = FakeInfluxClient(tables=[])
    pg = FakePgPool()
    health = FakeStorageHealth()

    service = TrendsService(influx, pg, health)

    response = await service.query(
        _request(stream_ids=["stream-a", "stream-b"])
    )

    assert response.fallback is False
    assert set(response.series.series.keys()) == {"stream-a", "stream-b"}
    assert response.series.series["stream-a"] == []
    assert response.series.series["stream-b"] == []
