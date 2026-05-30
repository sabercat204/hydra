"""Exposure dedup invariance property test (task 7.14).

Property 8 — *Exposure dedup invariance*. For any multiset of identical
``(asset_id, record_hash, matched_indicator)`` triples, the resulting row
count in ``asset_exposures`` is at most 1. This is the API-visible
consequence of the partial unique index ``idx_asset_exposures_dedup`` plus
the repository's ``ON CONFLICT DO NOTHING`` clause.

Validates: R3.3, R3.5, R27.10.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from hypothesis import given, settings as h_settings, strategies as st

from hydra.eas.assets.repository import ExposureRepository


# ---------------------------------------------------------------------------
# FakePool — in-memory asset_exposures table with ON CONFLICT DO NOTHING
# ---------------------------------------------------------------------------


class _FakeConnection:
    """Duck-typed asyncpg ``Connection`` used by :class:`_FakePool`.

    Implements only :meth:`fetchval` because
    :meth:`ExposureRepository.insert_exposure` is the sole call site
    exercised by the tests in this file. The SQL produced by the
    repository is a static ``INSERT ... ON CONFLICT DO NOTHING RETURNING
    exposure_id``, so we don't parse the SQL text at all — we rely on
    the positional parameters in the order the repository binds them:
    ``(asset_id, tenant_id, record_hash, tier, matched_indicator,
    severity)``.
    """

    def __init__(self, pool: "_FakePool") -> None:
        self._pool = pool

    async def fetchval(self, sql: str, *params: Any) -> UUID | None:
        # Defensive sanity check: make sure we are in the ON CONFLICT
        # DO NOTHING path. If the repository SQL ever changes, this
        # guard fires loudly so the test can be updated instead of
        # silently drifting.
        assert re.search(
            r"ON\s+CONFLICT\s*\(\s*asset_id\s*,\s*record_hash\s*,\s*matched_indicator\s*\)\s*DO\s+NOTHING",
            sql,
            re.IGNORECASE,
        ), "FakeConnection.fetchval expected ON CONFLICT DO NOTHING INSERT"

        (
            asset_id,
            tenant_id,
            record_hash,
            tier,
            matched_indicator,
            severity,
        ) = params

        # Natural key for the partial unique index.
        key = (asset_id, record_hash, matched_indicator)
        for row in self._pool.rows:
            if (row["asset_id"], row["record_hash"], row["matched_indicator"]) == key:
                # Conflict: ON CONFLICT DO NOTHING ⇒ RETURNING yields
                # no row ⇒ ``fetchval`` returns ``None``.
                return None

        exposure_id = uuid4()
        self._pool.rows.append(
            {
                "exposure_id": exposure_id,
                "asset_id": asset_id,
                "tenant_id": tenant_id,
                "record_hash": record_hash,
                "tier": int(tier),
                "matched_indicator": matched_indicator,
                "severity": severity,
                "created_at": datetime.now(timezone.utc),
            }
        )
        return exposure_id


class _FakePool:
    """In-memory stand-in for ``asyncpg.Pool`` for the exposures table.

    Stores rows in a plain list so tests can ``len(pool.rows)`` after
    each call. ``acquire()`` yields a :class:`_FakeConnection` via the
    async context-manager protocol.
    """

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def acquire(self) -> "_FakePool":
        return self

    async def __aenter__(self) -> _FakeConnection:
        return _FakeConnection(self)

    async def __aexit__(self, *exc: Any) -> None:
        return None


# ---------------------------------------------------------------------------
# Property 8 — multiset of identical triples collapses to at most one row
# ---------------------------------------------------------------------------


@given(n=st.integers(min_value=1, max_value=20))
@h_settings(max_examples=50, deadline=None)
async def test_property_dedup(n: int) -> None:
    """Property 8 — N identical inserts produce exactly one row.

    The repository's ``ON CONFLICT (asset_id, record_hash,
    matched_indicator) DO NOTHING`` clause must collapse the multiset
    of identical triples to at most one row. We use ``n ∈ [1, 20]``
    because the property holds for any positive N; a few repetitions
    are enough to exercise the conflict path and larger values slow
    the test without adding information.

    Validates: R3.3, R3.5, R27.10.
    """
    pool = _FakePool()
    repo = ExposureRepository(pool)

    # Freeze the triple up-front so every call uses bitwise-identical
    # inputs. ``severity`` and ``tier`` are in the INSERT VALUES too,
    # but the unique index is on the triple only, so these can stay
    # constant without loss of generality.
    asset_id = uuid4()
    tenant_id = uuid4()
    record_hash = "0123456789abcdef"
    matched_indicator = "192.0.2.1"

    results: list[UUID | None] = []
    for _ in range(n):
        result = await repo.insert_exposure(
            asset_id=asset_id,
            tenant_id=tenant_id,
            record_hash=record_hash,
            tier=16,
            matched_indicator=matched_indicator,
            severity="high",
        )
        results.append(result)

    # Exactly one row persisted, regardless of N.
    assert len(pool.rows) == 1

    # The first call returns a fresh UUID; subsequent calls return
    # ``None`` from the ON CONFLICT path.
    assert isinstance(results[0], UUID)
    for r in results[1:]:
        assert r is None


# ---------------------------------------------------------------------------
# Positive control — N distinct triples produce N rows
# ---------------------------------------------------------------------------


async def test_dedup_distinct_triples_all_inserted() -> None:
    """Positive control — distinct triples are not deduped.

    This complements the dedup property: the ``ON CONFLICT`` path must
    only collapse rows whose natural key actually matches. Five
    distinct triples ⇒ five distinct rows.
    """
    pool = _FakePool()
    repo = ExposureRepository(pool)

    tenant_id = uuid4()
    results: list[UUID | None] = []
    for i in range(5):
        result = await repo.insert_exposure(
            asset_id=uuid4(),  # distinct asset → distinct triple
            tenant_id=tenant_id,
            record_hash=f"{i:016x}",
            tier=16,
            matched_indicator=f"192.0.2.{i}",
            severity="high",
        )
        results.append(result)

    assert len(pool.rows) == 5
    assert all(isinstance(r, UUID) for r in results)
    # All ids unique — nothing deduped incorrectly.
    assert len({r for r in results}) == 5


async def test_dedup_same_triple_different_severity_still_deduped() -> None:
    """Changing ``severity`` alone does NOT bypass dedup.

    The partial unique index is on ``(asset_id, record_hash,
    matched_indicator)`` — severity is not part of the key. The second
    insert with a different severity but the same triple must be a
    no-op (ON CONFLICT DO NOTHING).
    """
    pool = _FakePool()
    repo = ExposureRepository(pool)

    asset_id = uuid4()
    tenant_id = uuid4()

    first = await repo.insert_exposure(
        asset_id=asset_id,
        tenant_id=tenant_id,
        record_hash="0123456789abcdef",
        tier=16,
        matched_indicator="192.0.2.1",
        severity="high",
    )
    second = await repo.insert_exposure(
        asset_id=asset_id,
        tenant_id=tenant_id,
        record_hash="0123456789abcdef",
        tier=16,
        matched_indicator="192.0.2.1",
        severity="critical",  # different, but irrelevant
    )

    assert isinstance(first, UUID)
    assert second is None
    assert len(pool.rows) == 1
    # The persisted severity is the one from the first insert; the
    # second call's severity never reached the table because ON
    # CONFLICT DO NOTHING skipped the UPDATE path.
    assert pool.rows[0]["severity"] == "high"
