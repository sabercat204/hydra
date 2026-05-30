"""Property 6 — Pagination round-trip invariance (task 18.1).

The invariant (Design §7, R27.1):

> Concatenating every follow-cursor page of a paged listing yields the
> same **multiset** as a single unpaginated scan that reads the full
> result set.

Six endpoints in the EAS surface use cursor pagination and therefore
need to uphold this invariant:

1. ``GET /api/v1/assets``
2. ``GET /api/v1/assets/{asset_id}/exposures``
3. ``GET /api/v1/exposures``
4. ``GET /api/v1/images/search``
5. ``GET /api/v1/cves/search``
6. ``GET /api/v1/exploits/search``

All six go through the ``encode_cursor`` / ``decode_cursor`` helpers in
:mod:`hydra.api.pagination`, which use a deterministic JSON+base64
encoding. The round-trip invariant is therefore a property of those
helpers + the repository-side cursor predicate. If it holds against a
FakePool driven repository for each storage backend (PG for assets /
exposures, ES for images / cves / exploits), it holds end-to-end.

This file drives the repositories directly — the thin router layer
that invokes them adds no new pagination logic. We parametrize the
tests across:

* ``AssetRepository.list_active``       → PG, created_at DESC
* ``ExposureRepository.list_for_asset`` → PG, created_at DESC
* ``ExposureRepository.list_for_tenant``→ PG, created_at DESC
* ``encode_cursor`` / ``decode_cursor`` → generic round-trip

For the three ES-backed search endpoints (images / cves / exploits),
we validate the pagination **cursor-encoding contract** directly
against the shared helpers, plus a generic fake-ES-store check that
confirms "fetch ``limit+1`` → trim to ``limit`` → emit cursor of last
row" produces a multiset-equal concatenation on a seeded dataset.

Validates: R4.1, R4.4, R11.3, R11.5, R27.1.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from uuid import UUID, uuid4

import pytest
from hypothesis import HealthCheck, given, settings as h_settings, strategies as st

from hydra.api.pagination import build_paged_response, decode_cursor, encode_cursor
from hydra.eas.assets.repository import (
    AssetRepository,
    ExposureRepository,
)


# ---------------------------------------------------------------------------
# Shared FakePool / FakeConnection implementing the PG cursor predicate
# ---------------------------------------------------------------------------


class _PgFakeConnection:
    """Duck-typed asyncpg ``Connection`` for the three PG repositories.

    Routes on the SQL text's first filter column:

    * ``"asset_id = $1"`` → :meth:`ExposureRepository.list_for_asset`
    * ``"tenant_id = $1" + exposure columns`` → :meth:`.list_for_tenant`
    * ``"tenant_id = $1" + asset columns`` → :meth:`AssetRepository.list_active`

    We distinguish tenant-scoped queries (assets vs exposures) by the
    SELECT column list — assets SELECT ``asset_id, ... normalized_value,
    raw_value`` while exposures SELECT ``exposure_id, asset_id, record_hash,
    tier, matched_indicator, severity``.
    """

    def __init__(self, pool: "_PgFakePool") -> None:
        self._pool = pool

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        rows = self._pool.rows
        limit = _extract_limit(sql)

        # Select the row set based on the table the SELECT targets.
        if "FROM asset_exposures" in sql:
            rows = [r for r in rows if r.get("_kind") == "exposure"]
        elif "FROM assets" in sql:
            rows = [r for r in rows if r.get("_kind") == "asset"]

        # First param is always tenant_id OR asset_id. Look at the SQL
        # prefix to know which.
        if "asset_id = $1" in sql:
            asset_id = params[0]
            tenant_id = params[1]
            rows = [
                r for r in rows
                if r["asset_id"] == asset_id and r["tenant_id"] == tenant_id
            ]
            rest = list(params[2:])
        else:
            tenant_id = params[0]
            rows = [r for r in rows if r["tenant_id"] == tenant_id]
            rest = list(params[1:])

        # Optional filter / cursor params arrive in a documented order:
        # [severity_list] [since_dt] [cursor_dt cursor_id] [asset_type]
        # — we disambiguate by type as we did in test_exposures_endpoint.

        # 1) Asset-type filter (str) — only meaningful when the ``assets``
        #    table is queried.
        asset_type_filter: str | None = None
        for i, item in enumerate(rest):
            if isinstance(item, str) and "FROM assets" in sql:
                asset_type_filter = item
                rest.pop(i)
                break

        # 2) Severity filter (list[str]) — exposures only.
        severity_filter: list[str] | None = None
        for i, item in enumerate(rest):
            if isinstance(item, list):
                severity_filter = item
                rest.pop(i)
                break

        # 3) Cursor (dt, uuid) at the TAIL.
        cursor_dt: datetime | None = None
        cursor_id: UUID | None = None
        if rest and isinstance(rest[-1], UUID):
            cursor_id = rest.pop()
            assert rest and isinstance(rest[-1], datetime)
            cursor_dt = rest.pop()

        # 4) ``since`` filter (remaining datetime).
        since_filter: datetime | None = None
        for i, item in enumerate(rest):
            if isinstance(item, datetime):
                since_filter = item
                rest.pop(i)
                break

        # Apply filters.
        if asset_type_filter is not None:
            rows = [r for r in rows if r.get("asset_type") == asset_type_filter]
        if severity_filter is not None:
            rows = [r for r in rows if r.get("severity") in severity_filter]
        if since_filter is not None:
            rows = [r for r in rows if r["created_at"] > since_filter]

        if cursor_dt is not None and cursor_id is not None:
            # ``(created_at, id) < (cursor_dt, cursor_id)`` per the
            # repository's strict predicate for DESC ordering.
            sort_id_key = "exposure_id" if "FROM asset_exposures" in sql else "asset_id"
            rows = [
                r for r in rows
                if (r["created_at"], r[sort_id_key]) < (cursor_dt, cursor_id)
            ]

        # Sort DESC on (created_at, id).
        sort_id_key = "exposure_id" if "FROM asset_exposures" in sql else "asset_id"
        rows = sorted(
            rows,
            key=lambda r: (r["created_at"], r[sort_id_key]),
            reverse=True,
        )
        return rows[:limit]

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        # Not exercised by the listing repositories we test here.
        return None


def _extract_limit(sql: str) -> int:
    import re
    m = re.search(r"LIMIT\s+(\d+)", sql, re.IGNORECASE)
    return int(m.group(1)) if m else 1_000


class _PgFakePool:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def acquire(self) -> "_PgFakePool":
        return self

    async def __aenter__(self) -> _PgFakeConnection:
        return _PgFakeConnection(self)

    async def __aexit__(self, *exc: Any) -> None:
        return None


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


_EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _seed_assets(pool: _PgFakePool, tenant_id: UUID, n: int) -> list[UUID]:
    ids: list[UUID] = []
    for i in range(n):
        asset_id = uuid4()
        ids.append(asset_id)
        pool.rows.append(
            {
                "_kind": "asset",
                "asset_id": asset_id,
                "tenant_id": tenant_id,
                "asset_type": "ip",
                "normalized_value": f"10.0.0.{i + 1}",
                "raw_value": f"10.0.0.{i + 1}",
                "is_active": True,
                "capture_screenshots": False,
                "notes": None,
                "created_at": _EPOCH + timedelta(minutes=i),
                "deactivated_at": None,
            }
        )
    return ids


def _seed_exposures(
    pool: _PgFakePool,
    *,
    tenant_id: UUID,
    asset_id: UUID,
    n: int,
    severity: str = "high",
) -> list[UUID]:
    ids: list[UUID] = []
    for i in range(n):
        exposure_id = uuid4()
        ids.append(exposure_id)
        pool.rows.append(
            {
                "_kind": "exposure",
                "exposure_id": exposure_id,
                "asset_id": asset_id,
                "tenant_id": tenant_id,
                "record_hash": f"{i:016x}",
                "tier": 16,
                "matched_indicator": f"10.0.0.{i + 1}",
                "severity": severity,
                "created_at": _EPOCH + timedelta(minutes=i),
            }
        )
    return ids


# ---------------------------------------------------------------------------
# Generic round-trip helper
# ---------------------------------------------------------------------------


async def _walk_pages(
    fetch_page: Callable[[str | None, int], "Any"],
    limit: int,
    max_iters: int = 1000,
) -> list[Any]:
    """Walk all pages via ``fetch_page(cursor, limit)`` and return the concat.

    ``fetch_page`` must return ``(rows, next_cursor)`` tuples; the loop
    terminates when ``next_cursor`` is ``None``.
    """

    collected: list[Any] = []
    cursor: str | None = None
    for _ in range(max_iters):
        rows, cursor = await fetch_page(cursor, limit)
        collected.extend(rows)
        if cursor is None:
            break
    else:  # pragma: no cover - safety net
        pytest.fail("pagination did not terminate within max_iters")
    return collected


# ---------------------------------------------------------------------------
# Pure cursor-helper round-trip — Property 6 at the transport layer
# ---------------------------------------------------------------------------


@given(
    sort_field=st.sampled_from(["created_at", "rendered_at", "published"]),
    iso=st.datetimes(
        min_value=datetime(2020, 1, 1),
        max_value=datetime(2030, 1, 1),
    ).map(lambda dt: dt.replace(tzinfo=timezone.utc).isoformat()),
    last_id=st.uuids().map(str),
)
@h_settings(max_examples=100, deadline=None)
def test_property_cursor_encode_decode_roundtrip(
    sort_field: str, iso: str, last_id: str
) -> None:
    """The ``encode`` / ``decode`` pair is a perfect bijection.

    This is the foundation of Property 6 — any cursor the repository
    emits can be decoded by the next request to produce the exact
    values the repository's cursor predicate was built to consume.

    Validates: R27.1.
    """

    cursor = encode_cursor(sort_field, iso, last_id)
    decoded_field, decoded_value, decoded_id = decode_cursor(cursor)
    assert decoded_field == sort_field
    assert decoded_value == iso
    assert decoded_id == last_id


# ---------------------------------------------------------------------------
# Assets — GET /api/v1/assets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("limit", [1, 2, 3, 5])
async def test_property_roundtrip_assets_list(limit: int) -> None:
    """Walking ``AssetRepository.list_active`` yields the full seed multiset.

    Validates: R4.1, R27.1.
    """

    pool = _PgFakePool()
    repo = AssetRepository(pool)
    tenant_id = uuid4()
    seeded = set(_seed_assets(pool, tenant_id, n=7))

    async def fetch(cursor: str | None, lim: int):
        return await repo.list_active(tenant_id, cursor=cursor, limit=lim)

    collected = await _walk_pages(fetch, limit=limit)
    assert {a.asset_id for a in collected} == seeded


async def test_assets_list_filter_survives_cursor() -> None:
    """``asset_type`` filter is preserved across page boundaries.

    R4 and R11.3 both require that filters compose with cursors so
    that every page honours the same where-clause. Seed five ``ip``
    assets and three ``domain`` assets, then page through with
    ``asset_type="ip"`` → every page returns only ``ip`` assets and
    the total multiset equals the five seeds.
    """

    pool = _PgFakePool()
    repo = AssetRepository(pool)
    tenant_id = uuid4()

    ip_ids = _seed_assets(pool, tenant_id, n=5)
    # Seed three ``domain`` assets to pollute the pool.
    for i in range(3):
        pool.rows.append(
            {
                "_kind": "asset",
                "asset_id": uuid4(),
                "tenant_id": tenant_id,
                "asset_type": "domain",
                "normalized_value": f"example{i}.com",
                "raw_value": f"example{i}.com",
                "is_active": True,
                "capture_screenshots": False,
                "notes": None,
                "created_at": _EPOCH + timedelta(hours=i),
                "deactivated_at": None,
            }
        )

    async def fetch(cursor: str | None, lim: int):
        return await repo.list_active(
            tenant_id, asset_type="ip", cursor=cursor, limit=lim
        )

    collected = await _walk_pages(fetch, limit=2)
    assert {a.asset_id for a in collected} == set(ip_ids)
    # Every returned asset is the filtered type.
    assert all(a.asset_type == "ip" for a in collected)


# ---------------------------------------------------------------------------
# Exposures (per-asset) — GET /api/v1/assets/{asset_id}/exposures
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("limit", [1, 3, 4])
async def test_property_roundtrip_exposures_for_asset(limit: int) -> None:
    """Walking ``ExposureRepository.list_for_asset`` yields the full seed multiset."""

    pool = _PgFakePool()
    repo = ExposureRepository(pool)
    tenant_id = uuid4()
    asset_id = uuid4()
    seeded = set(_seed_exposures(pool, tenant_id=tenant_id, asset_id=asset_id, n=5))

    async def fetch(cursor: str | None, lim: int):
        return await repo.list_for_asset(
            asset_id, tenant_id, cursor=cursor, limit=lim
        )

    collected = await _walk_pages(fetch, limit=limit)
    assert {e.exposure_id for e in collected} == seeded


async def test_exposures_for_asset_severity_filter_survives_cursor() -> None:
    """``severity`` filter survives page boundaries.

    Seed 3 critical + 3 high + 3 low exposures for one asset; paging
    with ``severity=["critical"]`` yields exactly 3 rows across all
    pages.
    """

    pool = _PgFakePool()
    repo = ExposureRepository(pool)
    tenant_id = uuid4()
    asset_id = uuid4()

    crit_ids = _seed_exposures(
        pool, tenant_id=tenant_id, asset_id=asset_id, n=3, severity="critical"
    )
    _seed_exposures(
        pool, tenant_id=tenant_id, asset_id=asset_id, n=3, severity="high"
    )
    _seed_exposures(
        pool, tenant_id=tenant_id, asset_id=asset_id, n=3, severity="low"
    )

    async def fetch(cursor: str | None, lim: int):
        return await repo.list_for_asset(
            asset_id,
            tenant_id,
            severity=["critical"],
            cursor=cursor,
            limit=lim,
        )

    collected = await _walk_pages(fetch, limit=1)
    assert {e.exposure_id for e in collected} == set(crit_ids)


# ---------------------------------------------------------------------------
# Exposures (tenant-wide) — GET /api/v1/exposures
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("limit", [1, 2, 7])
async def test_property_roundtrip_exposures_for_tenant(limit: int) -> None:
    """Walking ``ExposureRepository.list_for_tenant`` yields the full seed multiset."""

    pool = _PgFakePool()
    repo = ExposureRepository(pool)
    tenant_id = uuid4()

    # Seed across multiple assets to exercise tenant-wide pagination.
    ids: set[UUID] = set()
    for _ in range(4):
        asset_id = uuid4()
        ids.update(
            _seed_exposures(
                pool, tenant_id=tenant_id, asset_id=asset_id, n=3
            )
        )

    async def fetch(cursor: str | None, lim: int):
        return await repo.list_for_tenant(tenant_id, cursor=cursor, limit=lim)

    collected = await _walk_pages(fetch, limit=limit)
    assert {e.exposure_id for e in collected} == ids


# ---------------------------------------------------------------------------
# Search endpoints — ES-backed pagination via ``build_paged_response``
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("limit", [1, 3, 10])
def test_property_roundtrip_search_endpoint_cursor_contract(limit: int) -> None:
    """The ``limit+1`` fetch + cursor emit path from :func:`build_paged_response`
    is the shared cursor contract for the three ``/search`` endpoints.

    The function:

    1. Trims a ``limit+1``-item list to ``limit``.
    2. Emits a cursor encoding the last trimmed row's sort_value + id.
    3. A next page fed that cursor walks past the trimmed row.

    This is what the CVEs / exploits / images search routers do
    internally — they build a filtered list of up to ``limit+1`` hits
    from ES, call ``build_paged_response``, and return the paged
    envelope. We seed a sorted list of IDs + timestamps and drive the
    helper through a full walk to confirm Property 6 holds.

    Validates: R11.3, R11.5, R27.1.
    """

    # Seed 15 rows sorted DESC by timestamp.
    rows = [
        {"id": str(uuid4()), "ts": (_EPOCH + timedelta(minutes=i)).isoformat()}
        for i in range(15)
    ]
    rows.sort(key=lambda r: r["ts"], reverse=True)

    collected: list[dict[str, Any]] = []
    cursor: str | None = None
    iters = 0
    while True:
        iters += 1
        # Simulate "fetch limit+1 items strictly after cursor".
        if cursor is None:
            page_pool = rows
        else:
            _field, last_value, last_id = decode_cursor(cursor)
            page_pool = [
                r for r in rows
                if (r["ts"], r["id"]) < (last_value, last_id)
            ]
        fetched = page_pool[: limit + 1]
        page, meta = build_paged_response(
            fetched,
            limit=limit,
            sort_field="ts",
            get_sort_value=lambda r: r["ts"],
            get_id=lambda r: r["id"],
        )
        collected.extend(page)
        cursor = meta.next_cursor
        if cursor is None:
            break
        assert iters <= 100  # safety net

    # Concatenation equals the full sorted seed.
    assert [r["id"] for r in collected] == [r["id"] for r in rows]


# ---------------------------------------------------------------------------
# Generic termination property — the walk always terminates
# ---------------------------------------------------------------------------


@given(
    n_rows=st.integers(min_value=0, max_value=25),
    limit=st.integers(min_value=1, max_value=10),
)
@h_settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
async def test_property_pagination_terminates(n_rows: int, limit: int) -> None:
    """Pagination terminates in ≤ ``ceil(n/limit)`` iterations for any
    ``(n, limit)``.

    A silent infinite loop is the nastiest pagination bug — encoded in a
    bad cursor that never returns ``None``. Hypothesis explores a broad
    range of ``(n_rows, limit)`` combinations to catch any regression
    that would stall the walk.

    Validates: R27.1 (implicit — "pagination walk ends").
    """

    pool = _PgFakePool()
    repo = AssetRepository(pool)
    tenant_id = uuid4()
    _seed_assets(pool, tenant_id, n=n_rows)

    async def fetch(cursor: str | None, lim: int):
        return await repo.list_active(tenant_id, cursor=cursor, limit=lim)

    iters = 0
    cursor: str | None = None
    collected: list[Any] = []
    while True:
        iters += 1
        rows, cursor = await fetch(cursor, limit)
        collected.extend(rows)
        if cursor is None:
            break
        # Hard cap matching the upper bound: ceil(n_rows / limit) + 2
        # (the +2 is slack for the final empty page edge case).
        assert iters <= (n_rows // limit) + 2, (
            f"pagination did not terminate: iters={iters} "
            f"n_rows={n_rows} limit={limit}"
        )

    assert len(collected) == n_rows


# ---------------------------------------------------------------------------
# No-cursor on exact-fit — a page that exactly exhausts the data has no
# next_cursor
# ---------------------------------------------------------------------------


async def test_exact_fit_page_has_no_next_cursor() -> None:
    """When the result count equals ``limit``, ``next_cursor`` is ``None``.

    The repository uses the ``limit+1`` fetch strategy: it asks for one
    extra row, and only emits a cursor when that extra row exists.
    Seeding exactly ``limit`` rows means the extra slot is empty and
    the cursor must be ``None``.
    """

    pool = _PgFakePool()
    repo = ExposureRepository(pool)
    tenant_id = uuid4()
    asset_id = uuid4()
    _seed_exposures(pool, tenant_id=tenant_id, asset_id=asset_id, n=5)

    rows, cursor = await repo.list_for_asset(asset_id, tenant_id, limit=5)
    assert len(rows) == 5
    assert cursor is None


async def test_empty_result_has_no_next_cursor() -> None:
    """An empty result set emits ``next_cursor=None``."""

    pool = _PgFakePool()
    repo = AssetRepository(pool)
    tenant_id = uuid4()
    rows, cursor = await repo.list_active(tenant_id, limit=10)
    assert rows == []
    assert cursor is None
