"""Integration tests for the exposures endpoint repository (task 7.16).

Exercises :meth:`ExposureRepository.list_for_tenant` and
:meth:`ExposureRepository.list_for_asset` end-to-end against a
``FakePool`` seeded with in-memory rows. We deliberately skip
FastAPI's ``TestClient`` because the repository layer *is* the
interesting surface here — the router is a thin ``Depends`` wrapper
over these methods and adding FastAPI to the test would pull in a
large amount of unrelated dependency surface.

Scenarios (R4.1–R4.4, Property 5, Property 6):

* Severity subset filtering.
* ``since`` timestamp filtering.
* Tenant isolation across both ``list_for_tenant`` and
  ``list_for_asset``.
* Pagination round-trip: concat of pages equals the single-shot list.
* Combined filter + cursor: filters hold page-to-page.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from hydra.api.pagination import decode_cursor
from hydra.eas.assets.repository import ExposureRepository


# ---------------------------------------------------------------------------
# FakePool — stores rows in-memory and serves both repository methods
# ---------------------------------------------------------------------------


class _FakeConnection:
    """Duck-typed asyncpg ``Connection`` for the exposures endpoint tests.

    Only :meth:`fetch` is implemented. The implementation inspects the
    incoming SQL to decide which repository method is calling:

    * ``"asset_id = $1"`` in the SQL ⇒ :meth:`list_for_asset`. The
      first param is the asset_id, the second is the tenant_id, and
      any subsequent params are (in order) the optional severity list,
      since timestamp, cursor datetime, cursor uuid.
    * ``"tenant_id = $1"`` ⇒ :meth:`list_for_tenant`. The first param
      is the tenant_id and any subsequent params follow the same
      optional ordering.

    Cursor params are always a trailing (datetime, uuid) pair — we
    identify them by inspecting the tail of the param list, which
    disambiguates the ``since`` datetime from the cursor datetime
    without parsing the WHERE clause.
    """

    def __init__(self, pool: "_FakePool") -> None:
        self._pool = pool

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        is_asset_scoped = "asset_id = $1" in sql
        limit = _extract_limit(sql)

        if is_asset_scoped:
            asset_id = params[0]
            tenant_id = params[1]
            rest = list(params[2:])
            rows = [
                r
                for r in self._pool.rows
                if r["asset_id"] == asset_id and r["tenant_id"] == tenant_id
            ]
        else:
            tenant_id = params[0]
            rest = list(params[1:])
            rows = [r for r in self._pool.rows if r["tenant_id"] == tenant_id]

        # The repository binds optional filters in a strict order:
        # severity-list → since-datetime → cursor-datetime → cursor-uuid.
        # Cursor params are always a (datetime, uuid) pair and always
        # appear last, so we can identify them by consuming the tail:
        # a trailing UUID implies the preceding datetime is the cursor
        # datetime. Anything left over is the ``since`` filter.
        severity_filter = _pop_first_of_type(rest, list)

        cursor_dt: datetime | None = None
        cursor_id: UUID | None = None
        if rest and isinstance(rest[-1], UUID):
            cursor_id = rest.pop()
            assert rest and isinstance(rest[-1], datetime), (
                "cursor_id without preceding cursor_dt — binding order wrong"
            )
            cursor_dt = rest.pop()

        since_filter = _pop_first_of_type(rest, datetime)

        # Apply severity filter — ``severity = ANY($n::text[])``.
        if severity_filter is not None:
            rows = [r for r in rows if r["severity"] in severity_filter]

        # ``since`` is strictly greater than: ``created_at > $n``.
        if since_filter is not None:
            rows = [r for r in rows if r["created_at"] > since_filter]

        # Cursor predicate: ``(created_at, exposure_id) < (cursor_dt,
        # cursor_id)`` for DESC ordering.
        if cursor_dt is not None and cursor_id is not None:
            rows = [
                r
                for r in rows
                if (r["created_at"], r["exposure_id"]) < (cursor_dt, cursor_id)
            ]

        # Sort DESC on (created_at, exposure_id) — matches the
        # repository ORDER BY.
        rows.sort(
            key=lambda r: (r["created_at"], r["exposure_id"]),
            reverse=True,
        )

        # Repository fetches ``limit + 1``; we mirror that truncation.
        return rows[:limit]


def _extract_limit(sql: str) -> int:
    """Read the ``LIMIT N`` clause out of the static SQL.

    The repository interpolates ``int(limit) + 1`` directly into the
    SQL, so we can pull it back out with a tiny regex. Falls back to
    ``1_000`` — a sentinel that will never be reached by the fixtures
    here — if the pattern is missing.
    """

    import re

    m = re.search(r"LIMIT\s+(\d+)", sql, re.IGNORECASE)
    return int(m.group(1)) if m else 1_000


def _pop_first_of_type(items: list[Any], cls: type) -> Any | None:
    """Remove and return the first element that is an instance of ``cls``.

    Used for non-cursor filters (``severity`` list, ``since`` datetime)
    where there is at most one binding of each type after cursor
    params have been peeled off the tail. ``bool`` is excluded from
    the integer branch because ``isinstance(True, int)`` is True, but
    none of our filters use booleans anyway.
    """

    for i, item in enumerate(items):
        if isinstance(item, cls):
            return items.pop(i)
    return None


class _FakePool:
    """In-memory ``asyncpg.Pool`` stand-in for the exposures repository."""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def acquire(self) -> "_FakePool":
        return self

    async def __aenter__(self) -> _FakeConnection:
        return _FakeConnection(self)

    async def __aexit__(self, *exc: Any) -> None:
        return None


# ---------------------------------------------------------------------------
# Seed helper
# ---------------------------------------------------------------------------


_EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _seed_exposure(
    pool: _FakePool,
    *,
    asset_id: UUID,
    tenant_id: UUID,
    severity: str,
    created_at_offset_hours: float = 0.0,
    matched_indicator: str = "192.0.2.1",
    tier: int = 16,
) -> UUID:
    """Append an exposure row to the fake pool.

    ``created_at_offset_hours`` is measured from :data:`_EPOCH`, giving
    tests a stable timeline to reason about ``since`` filters and
    DESC ordering without touching wall-clock time.
    """

    exposure_id = uuid4()
    pool.rows.append(
        {
            "exposure_id": exposure_id,
            "asset_id": asset_id,
            "tenant_id": tenant_id,
            "record_hash": f"{len(pool.rows):016x}",
            "tier": tier,
            "matched_indicator": matched_indicator,
            "severity": severity,
            "created_at": _EPOCH + timedelta(hours=created_at_offset_hours),
        }
    )
    return exposure_id


# ---------------------------------------------------------------------------
# Scenario 1 — severity filter subsets correctly
# ---------------------------------------------------------------------------


async def test_severity_filter_subsets_correctly() -> None:
    """``severity=[...]`` returns exactly the matching subset (R4.2)."""

    pool = _FakePool()
    repo = ExposureRepository(pool)
    tenant_id = uuid4()
    asset_id = uuid4()

    for i in range(3):
        _seed_exposure(
            pool,
            asset_id=asset_id,
            tenant_id=tenant_id,
            severity="critical",
            created_at_offset_hours=i,
        )
    for i in range(3):
        _seed_exposure(
            pool,
            asset_id=asset_id,
            tenant_id=tenant_id,
            severity="high",
            created_at_offset_hours=3 + i,
        )
    for i in range(3):
        _seed_exposure(
            pool,
            asset_id=asset_id,
            tenant_id=tenant_id,
            severity="low",
            created_at_offset_hours=6 + i,
        )

    # No filter → all 9 rows.
    all_rows, _ = await repo.list_for_tenant(tenant_id, limit=100)
    assert len(all_rows) == 9

    # critical-only → 3 rows.
    crit_rows, _ = await repo.list_for_tenant(
        tenant_id, severity=["critical"], limit=100
    )
    assert len(crit_rows) == 3
    assert all(r.severity == "critical" for r in crit_rows)

    # critical + high → 6 rows, only those two severities.
    both, _ = await repo.list_for_tenant(
        tenant_id, severity=["critical", "high"], limit=100
    )
    assert len(both) == 6
    assert {r.severity for r in both} == {"critical", "high"}


# ---------------------------------------------------------------------------
# Scenario 2 — since filter is strict greater-than
# ---------------------------------------------------------------------------


async def test_since_filter_is_strict_greater_than() -> None:
    """``since`` is ``created_at > since`` — strict, not inclusive (R4.3)."""

    pool = _FakePool()
    repo = ExposureRepository(pool)
    tenant_id = uuid4()
    asset_id = uuid4()

    # Seed at t=0, t=1h, t=2h, t=3h.
    for offset in (0.0, 1.0, 2.0, 3.0):
        _seed_exposure(
            pool,
            asset_id=asset_id,
            tenant_id=tenant_id,
            severity="high",
            created_at_offset_hours=offset,
        )

    since = _EPOCH + timedelta(hours=2)  # exactly t=2h

    rows, _ = await repo.list_for_tenant(tenant_id, since=since, limit=100)

    # Only t=3h is strictly greater than t=2h.
    assert len(rows) == 1
    assert rows[0].created_at == _EPOCH + timedelta(hours=3)


# ---------------------------------------------------------------------------
# Scenario 3 — tenant isolation (R4.4, R20.4)
# ---------------------------------------------------------------------------


async def test_tenant_isolation_in_list_for_tenant() -> None:
    """``list_for_tenant`` only returns rows owned by the calling tenant."""

    pool = _FakePool()
    repo = ExposureRepository(pool)
    tenant_a = uuid4()
    tenant_b = uuid4()

    # 2 rows for tenant A.
    for i in range(2):
        _seed_exposure(
            pool,
            asset_id=uuid4(),
            tenant_id=tenant_a,
            severity="high",
            created_at_offset_hours=i,
        )
    # 3 rows for tenant B.
    for i in range(3):
        _seed_exposure(
            pool,
            asset_id=uuid4(),
            tenant_id=tenant_b,
            severity="high",
            created_at_offset_hours=10 + i,
        )

    rows_a, _ = await repo.list_for_tenant(tenant_a, limit=100)
    rows_b, _ = await repo.list_for_tenant(tenant_b, limit=100)

    assert len(rows_a) == 2
    assert len(rows_b) == 3
    assert all(r.tenant_id == tenant_a for r in rows_a)
    assert all(r.tenant_id == tenant_b for r in rows_b)


async def test_tenant_isolation_in_list_for_asset_no_cross_tenant_leakage() -> None:
    """Passing tenant_a with an asset_id owned by tenant_b yields empty.

    This is the API-visible shape of the tenant-scoped WHERE clause in
    :meth:`ExposureRepository.list_for_asset`. The router uses this
    behaviour to return 404 for cross-tenant ``asset_id`` lookups.
    """

    pool = _FakePool()
    repo = ExposureRepository(pool)
    tenant_a = uuid4()
    tenant_b = uuid4()
    asset_b = uuid4()

    # Seed exposures for tenant_b's asset.
    for i in range(3):
        _seed_exposure(
            pool,
            asset_id=asset_b,
            tenant_id=tenant_b,
            severity="high",
            created_at_offset_hours=i,
        )

    # Tenant A asks for Tenant B's asset — must see nothing.
    rows, cursor = await repo.list_for_asset(asset_b, tenant_a, limit=100)
    assert rows == []
    assert cursor is None

    # Tenant B asking for their own asset sees the full 3 rows.
    own_rows, _ = await repo.list_for_asset(asset_b, tenant_b, limit=100)
    assert len(own_rows) == 3


# ---------------------------------------------------------------------------
# Scenario 4 — pagination round-trip (Property 6)
# ---------------------------------------------------------------------------


async def test_pagination_round_trip_yields_all_rows() -> None:
    """Property 6 — concat of pages equals the single-shot list."""

    pool = _FakePool()
    repo = ExposureRepository(pool)
    tenant_id = uuid4()

    seeded_ids: list[UUID] = []
    for i in range(5):
        seeded_ids.append(
            _seed_exposure(
                pool,
                asset_id=uuid4(),
                tenant_id=tenant_id,
                severity="high",
                created_at_offset_hours=i,
            )
        )

    # Page 1 — limit=2.
    page1, cursor1 = await repo.list_for_tenant(tenant_id, limit=2)
    assert len(page1) == 2
    assert cursor1 is not None

    # The cursor decodes to (sort_field="created_at", iso_value, id_str).
    field, _, _ = decode_cursor(cursor1)
    assert field == "created_at"

    # Page 2 — same limit, feed back the cursor.
    page2, cursor2 = await repo.list_for_tenant(
        tenant_id, limit=2, cursor=cursor1
    )
    assert len(page2) == 2
    assert cursor2 is not None

    # Page 3 — only 1 row left, no further cursor.
    page3, cursor3 = await repo.list_for_tenant(
        tenant_id, limit=2, cursor=cursor2
    )
    assert len(page3) == 1
    assert cursor3 is None

    # Multiset equality between concatenation of pages and the full fetch.
    full, _ = await repo.list_for_tenant(tenant_id, limit=100)
    paged = page1 + page2 + page3
    assert len(paged) == len(full) == 5
    assert {r.exposure_id for r in paged} == {r.exposure_id for r in full}
    assert {r.exposure_id for r in paged} == set(seeded_ids)


# ---------------------------------------------------------------------------
# Scenario 5 — filter + cursor compose correctly
# ---------------------------------------------------------------------------


async def test_filter_plus_cursor_preserves_filter_across_pages() -> None:
    """Severity filter survives the cursor round-trip.

    With three critical exposures alongside others at non-critical
    severities, a paged fetch with ``severity=["critical"]`` and
    ``limit=1`` should walk exactly three pages, each carrying only
    critical rows, terminating with ``next_cursor=None``.
    """

    pool = _FakePool()
    repo = ExposureRepository(pool)
    tenant_id = uuid4()
    asset_id = uuid4()

    # 3 criticals interleaved with 2 mediums so the filter has to do work.
    for offset in (0.0, 1.0, 2.0):
        _seed_exposure(
            pool,
            asset_id=asset_id,
            tenant_id=tenant_id,
            severity="critical",
            created_at_offset_hours=offset,
        )
    for offset in (0.5, 1.5):
        _seed_exposure(
            pool,
            asset_id=asset_id,
            tenant_id=tenant_id,
            severity="medium",
            created_at_offset_hours=offset,
        )

    collected: list[Any] = []
    cursor: str | None = None
    iterations = 0
    while True:
        iterations += 1
        rows, cursor = await repo.list_for_tenant(
            tenant_id, severity=["critical"], limit=1, cursor=cursor
        )
        collected.extend(rows)
        if cursor is None:
            break
        # Safety net — prevent runaway loops if something is wrong.
        assert iterations <= 10

    # 3 critical rows across 3 pages.
    assert iterations == 3
    assert len(collected) == 3
    assert {r.severity for r in collected} == {"critical"}


# ---------------------------------------------------------------------------
# Edge — limit exactly equal to row count yields no cursor
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("exact_match", [True, False])
async def test_no_cursor_when_fewer_or_equal_rows_than_limit(
    exact_match: bool,
) -> None:
    """When the result set fits in a single page, no cursor is emitted."""

    pool = _FakePool()
    repo = ExposureRepository(pool)
    tenant_id = uuid4()

    for i in range(3):
        _seed_exposure(
            pool,
            asset_id=uuid4(),
            tenant_id=tenant_id,
            severity="low",
            created_at_offset_hours=i,
        )

    limit = 3 if exact_match else 10
    rows, cursor = await repo.list_for_tenant(tenant_id, limit=limit)
    assert len(rows) == 3
    assert cursor is None
