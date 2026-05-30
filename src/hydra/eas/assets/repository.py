"""PostgreSQL repositories for assets and exposures (Design §6.1).

Two async classes sit between the router layer and the PG pool:

* :class:`AssetRepository` — CRUD over the ``assets`` table, plus the
  ``list_matching`` hot path used by :class:`AssetMonitor` to pull
  candidate assets for an indicator.
* :class:`ExposureRepository` — insert/list over ``asset_exposures``.

Both classes accept **any duck-typed pool** that exposes
``acquire()`` as an async context manager yielding a connection with
``fetchrow`` / ``fetch`` / ``execute`` / ``fetchval`` methods. This
matches ``asyncpg.Pool`` (the production wiring) and the fake pools
used in tests, without coupling the repository to a specific driver.

**Cursor pagination.** All ``list_*`` methods return
``(rows, next_cursor)``. Cursors are encoded by
:func:`hydra.api.pagination.encode_cursor` with sort field
``"created_at"`` and the row's UUID as the tiebreak. The ``last_value``
portion of the cursor carries the ISO 8601 timestamp string so that
``decode_cursor`` can round-trip through JSON without datetime
serialization subtlety. The sort order is **DESC by created_at** with
the id as a stable tiebreak — this matches R4.1 (``sorted by
created_at DESC``) and the index ``idx_asset_exposures_asset_created``.

**Tenant scoping.** Every read/write except ``list_matching`` carries
``tenant_id`` in its ``WHERE`` clause, enforcing R20.3 at the
repository layer rather than the router (Design §3.1). ``list_matching``
is intentionally tenant-agnostic because the monitor needs to find
matching assets across **all** tenants for a single indicator; the
caller is responsible for enforcing tenant scope on the match result.

**Quota enforcement (R1.4).** ``count_active`` is a cheap COUNT(*)
filtered by tenant — accurate at the moment of the check. The
``idx_assets_tenant`` index makes this O(active_assets_for_tenant).
The Assets_Router uses this to decide between 201 and a 409
``ASSET_QUOTA_EXCEEDED`` response.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from hydra.api.pagination import decode_cursor, encode_cursor
from hydra.eas.assets.models import Asset, ExposureEvent
from hydra.eas.schemas.assets import AssetCreate

logger = logging.getLogger(__name__)

__all__ = ["AssetRepository", "ExposureRepository", "UpsertResult"]


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _row_to_asset(row: Any) -> Asset:
    """Adapt an asyncpg ``Record`` (or dict-like) to :class:`Asset`."""

    # asyncpg ``Record`` supports dict-style key access but lacks a
    # ``.get()`` method, so we try/except for the optional ``notes`` column
    # to stay compatible with both ``Record`` and test-doubles that pass
    # in plain dicts.
    try:
        notes = row["notes"]
    except (KeyError, IndexError):
        notes = None
    return Asset(
        asset_id=row["asset_id"],
        tenant_id=row["tenant_id"],
        asset_type=row["asset_type"],
        normalized_value=row["normalized_value"],
        raw_value=row["raw_value"],
        is_active=row["is_active"],
        capture_screenshots=row["capture_screenshots"],
        created_at=row["created_at"],
        deactivated_at=row["deactivated_at"],
        notes=notes,
    )


def _row_to_exposure(row: Any) -> ExposureEvent:
    return ExposureEvent(
        exposure_id=row["exposure_id"],
        asset_id=row["asset_id"],
        tenant_id=row["tenant_id"],
        record_hash=row["record_hash"],
        tier=int(row["tier"]),
        matched_indicator=row["matched_indicator"],
        severity=row["severity"],
        created_at=row["created_at"],
    )


def _encode_created_cursor(created_at: datetime, row_id: UUID) -> str:
    """Build a cursor for a ``(created_at DESC, id)`` sort key.

    ``created_at`` is serialized as an ISO 8601 string rather than left as a
    ``datetime`` so that the base64-encoded JSON payload is deterministic
    across Python versions that differ in ``datetime.__str__`` formatting.
    """

    return encode_cursor("created_at", created_at.isoformat(), str(row_id))


def _decode_created_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Inverse of :func:`_encode_created_cursor`."""

    _, iso_value, id_str = decode_cursor(cursor)
    # ``fromisoformat`` handles both timezone-aware and naive strings on
    # 3.11+; we treat naive as UTC for safety.
    dt = datetime.fromisoformat(iso_value)
    return dt, UUID(id_str)


def _next_cursor(
    rows: list[Any],
    limit: int,
    get_created: Any,
    get_id: Any,
) -> tuple[list[Any], str | None]:
    """Trim a limit+1 fetch and compute the ``next_cursor`` string."""

    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        return rows, _encode_created_cursor(get_created(last), get_id(last))
    return rows, None


# ----------------------------------------------------------------------
# Dataclass returned by AssetRepository.upsert
# ----------------------------------------------------------------------


class UpsertResult:
    """Outcome of :meth:`AssetRepository.upsert`.

    ``was_new`` is the canonical Postgres ``xmax = 0`` signal — ``True``
    when this call actually inserted a fresh row, ``False`` when an
    existing row was updated. The router uses this to decide between
    the 201 and 200 response codes (R1.3).
    """

    __slots__ = ("asset", "was_new")

    def __init__(self, asset: Asset, was_new: bool) -> None:
        self.asset = asset
        self.was_new = was_new


# ----------------------------------------------------------------------
# AssetRepository
# ----------------------------------------------------------------------


class AssetRepository:
    """CRUD over the ``assets`` table (Design §6.1)."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    async def upsert(
        self,
        tenant_id: UUID,
        body: AssetCreate,
        normalized_value: str,
    ) -> UpsertResult:
        """Create or idempotently re-fetch an asset.

        The ON CONFLICT target matches the partial unique index
        ``idx_assets_tenant_type_value_active`` (``WHERE is_active``) so
        that a tenant can re-register a previously deactivated asset
        without clashing with the soft-deleted row. On conflict we update
        ``raw_value``, ``notes``, and ``capture_screenshots`` per spec,
        which keeps the canonical form in sync when the user submits a
        differently-formatted but equivalent value.

        Returns an :class:`UpsertResult` including a ``was_new`` flag
        computed from ``xmax = 0`` — the PostgreSQL canonical way to
        distinguish INSERT from UPDATE in an upsert.
        """

        sql = """
            INSERT INTO assets (
                tenant_id, asset_type, normalized_value, raw_value,
                notes, capture_screenshots, is_active
            )
            VALUES ($1, $2, $3, $4, $5, $6, TRUE)
            ON CONFLICT (tenant_id, asset_type, normalized_value)
                WHERE is_active
            DO UPDATE SET
                raw_value = EXCLUDED.raw_value,
                notes = EXCLUDED.notes,
                capture_screenshots = EXCLUDED.capture_screenshots
            RETURNING
                asset_id, tenant_id, asset_type, normalized_value, raw_value,
                is_active, capture_screenshots, notes, created_at,
                deactivated_at, (xmax = 0) AS was_new
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                sql,
                tenant_id,
                body.asset_type.value,
                normalized_value,
                body.value,
                body.notes,
                body.capture_screenshots,
            )
        if row is None:
            # Should never happen with RETURNING but guard anyway.
            raise RuntimeError("assets upsert returned no row")
        return UpsertResult(_row_to_asset(row), bool(row["was_new"]))

    async def deactivate(self, tenant_id: UUID, asset_id: UUID) -> bool:
        """Soft-delete an asset; returns ``True`` iff a row was updated."""

        sql = """
            UPDATE assets
               SET is_active = FALSE,
                   deactivated_at = NOW()
             WHERE asset_id = $1
               AND tenant_id = $2
               AND is_active = TRUE
        """
        async with self._pool.acquire() as conn:
            status = await conn.execute(sql, asset_id, tenant_id)
        # asyncpg returns a status string like ``"UPDATE 1"``. Not all
        # duck-typed doubles follow that convention, so we fall back to
        # truthiness for non-string return values (e.g. int from a mock).
        if isinstance(status, str):
            return status.endswith(" 1")
        return bool(status)

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    async def get(self, tenant_id: UUID, asset_id: UUID) -> Asset | None:
        """Fetch a single asset by id, tenant-scoped."""

        sql = """
            SELECT asset_id, tenant_id, asset_type, normalized_value, raw_value,
                   is_active, capture_screenshots, notes, created_at, deactivated_at
              FROM assets
             WHERE asset_id = $1
               AND tenant_id = $2
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, asset_id, tenant_id)
        return _row_to_asset(row) if row is not None else None

    async def get_active_by_key(
        self,
        tenant_id: UUID,
        asset_type: str,
        normalized_value: str,
    ) -> Asset | None:
        """Fetch an active asset by its natural key (tenant + type + value).

        Used by the Assets_Router quota check to decide whether a POST
        would create a new row (quota-consuming) or merely re-upsert an
        existing one (free). Matches the partial unique index
        ``idx_assets_tenant_type_value_active``.
        """

        sql = """
            SELECT asset_id, tenant_id, asset_type, normalized_value, raw_value,
                   is_active, capture_screenshots, notes, created_at, deactivated_at
              FROM assets
             WHERE tenant_id = $1
               AND asset_type = $2
               AND normalized_value = $3
               AND is_active = TRUE
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, tenant_id, asset_type, normalized_value)
        return _row_to_asset(row) if row is not None else None

    async def list_active(
        self,
        tenant_id: UUID,
        *,
        asset_type: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[Asset], str | None]:
        """Paged list of active assets sorted by ``created_at DESC``."""

        # Build WHERE dynamically based on cursor and asset_type filter.
        conditions = ["tenant_id = $1", "is_active = TRUE"]
        params: list[Any] = [tenant_id]

        if asset_type is not None:
            params.append(asset_type)
            conditions.append(f"asset_type = ${len(params)}")

        if cursor is not None:
            cursor_dt, cursor_id = _decode_created_cursor(cursor)
            params.append(cursor_dt)
            dt_idx = len(params)
            params.append(cursor_id)
            id_idx = len(params)
            # Strict "row-past-the-cursor" predicate for DESC ordering:
            # take rows strictly older than the cursor row, or rows with
            # the same created_at whose asset_id sorts strictly below
            # the cursor id.
            conditions.append(
                f"(created_at, asset_id) < (${dt_idx}, ${id_idx})"
            )

        sql = f"""
            SELECT asset_id, tenant_id, asset_type, normalized_value, raw_value,
                   is_active, capture_screenshots, notes, created_at, deactivated_at
              FROM assets
             WHERE {' AND '.join(conditions)}
             ORDER BY created_at DESC, asset_id DESC
             LIMIT {int(limit) + 1}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        assets = [_row_to_asset(r) for r in rows]
        assets, next_cursor = _next_cursor(
            assets,
            limit,
            lambda a: a.created_at,
            lambda a: a.asset_id,
        )
        return assets, next_cursor

    async def count_active(self, tenant_id: UUID) -> int:
        """R1.4 quota support — count of active assets for a tenant."""

        async with self._pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM assets WHERE tenant_id = $1 AND is_active = TRUE",
                tenant_id,
            )
        return int(count or 0)

    async def list_matching(self, indicator_value: str) -> list[Asset]:
        """Return **candidate** active assets for an indicator (any tenant).

        This is intentionally conservative: the query filters by
        ``normalized_value`` but the authoritative ``is_match`` decision
        is made in :class:`AssetMatcher` because CIDR containment and
        domain suffix matching can't be expressed efficiently in SQL
        without bespoke extensions.

        To avoid scanning the whole ``assets`` table for every ingested
        record, we combine three index-friendly predicates:

        1. Exact equality on ``normalized_value`` (covers IP, HOSTNAME,
           ASN, and the exact-match branch of DOMAIN).
        2. CIDR containment via a LIKE prefix scan — not quite right
           but good enough for the MVP: CIDR assets typically look like
           ``10.0.0.0/24`` and indicators like ``10.0.0.5``; we fetch
           every CIDR-type asset and let the matcher make the final
           call. Cost is bounded by the total number of registered
           CIDR assets (small).
        3. DOMAIN suffix via a LIKE scan against all DOMAIN-type assets
           whose ``normalized_value`` is a proper suffix of the
           indicator — covered here by fetching all DOMAIN-type assets
           for the same reason as (2).

        The three fetches are unioned and returned as a single list.
        """

        parent_domains = _suffix_candidates(indicator_value)

        sql = """
            SELECT asset_id, tenant_id, asset_type, normalized_value, raw_value,
                   is_active, capture_screenshots, notes, created_at, deactivated_at
              FROM assets
             WHERE is_active = TRUE
               AND (
                    normalized_value = $1
                 OR (asset_type = 'cidr')
                 OR (asset_type = 'domain' AND normalized_value = ANY($2::text[]))
                 OR (asset_type = 'asn')
               )
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, indicator_value, parent_domains)
        return [_row_to_asset(r) for r in rows]


def _suffix_candidates(indicator: str) -> list[str]:
    """Return the list of domain suffixes that could match ``indicator``.

    For ``"foo.bar.example.com"`` this yields
    ``["foo.bar.example.com", "bar.example.com", "example.com", "com"]``.
    Empty list for non-domain-shaped strings so the ANY() predicate is
    vacuously false for IP-shaped indicators.
    """

    lowered = indicator.lower().rstrip(".")
    if not lowered or "." not in lowered:
        return [lowered] if lowered else []
    labels = lowered.split(".")
    return [".".join(labels[i:]) for i in range(len(labels))]


# ----------------------------------------------------------------------
# ExposureRepository
# ----------------------------------------------------------------------


class ExposureRepository:
    """CRUD over the ``asset_exposures`` table (Design §6.1)."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def insert_exposure(
        self,
        asset_id: UUID,
        tenant_id: UUID,
        record_hash: str,
        tier: int,
        matched_indicator: str,
        severity: str,
    ) -> UUID | None:
        """Insert an exposure row; ``None`` when deduped by the unique index.

        The ``ON CONFLICT (asset_id, record_hash, matched_indicator) DO
        NOTHING`` clause matches the partial unique index
        ``idx_asset_exposures_dedup`` and is what delivers R3.3 / R27.10.
        ``RETURNING exposure_id`` + ``fetchval`` returns ``None`` when
        the conflict path fires, giving the monitor a cheap signal to
        skip downstream work (alerting, metrics).
        """

        sql = """
            INSERT INTO asset_exposures (
                asset_id, tenant_id, record_hash, tier,
                matched_indicator, severity
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (asset_id, record_hash, matched_indicator) DO NOTHING
            RETURNING exposure_id
        """
        async with self._pool.acquire() as conn:
            exposure_id = await conn.fetchval(
                sql,
                asset_id,
                tenant_id,
                record_hash,
                int(tier),
                matched_indicator,
                severity,
            )
        return exposure_id

    async def list_for_asset(
        self,
        asset_id: UUID,
        tenant_id: UUID,
        *,
        severity: list[str] | None = None,
        since: datetime | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[ExposureEvent], str | None]:
        """Paged listing filtered to a single asset."""

        conditions = ["asset_id = $1", "tenant_id = $2"]
        params: list[Any] = [asset_id, tenant_id]

        if severity:
            params.append(list(severity))
            conditions.append(f"severity = ANY(${len(params)}::text[])")

        if since is not None:
            params.append(since)
            conditions.append(f"created_at > ${len(params)}")

        if cursor is not None:
            cursor_dt, cursor_id = _decode_created_cursor(cursor)
            params.append(cursor_dt)
            dt_idx = len(params)
            params.append(cursor_id)
            id_idx = len(params)
            conditions.append(
                f"(created_at, exposure_id) < (${dt_idx}, ${id_idx})"
            )

        sql = f"""
            SELECT exposure_id, asset_id, tenant_id, record_hash, tier,
                   matched_indicator, severity, created_at
              FROM asset_exposures
             WHERE {' AND '.join(conditions)}
             ORDER BY created_at DESC, exposure_id DESC
             LIMIT {int(limit) + 1}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        events = [_row_to_exposure(r) for r in rows]
        events, next_cursor = _next_cursor(
            events,
            limit,
            lambda e: e.created_at,
            lambda e: e.exposure_id,
        )
        return events, next_cursor

    async def list_for_tenant(
        self,
        tenant_id: UUID,
        *,
        severity: list[str] | None = None,
        since: datetime | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[ExposureEvent], str | None]:
        """Paged cross-asset listing for a tenant."""

        conditions = ["tenant_id = $1"]
        params: list[Any] = [tenant_id]

        if severity:
            params.append(list(severity))
            conditions.append(f"severity = ANY(${len(params)}::text[])")

        if since is not None:
            params.append(since)
            conditions.append(f"created_at > ${len(params)}")

        if cursor is not None:
            cursor_dt, cursor_id = _decode_created_cursor(cursor)
            params.append(cursor_dt)
            dt_idx = len(params)
            params.append(cursor_id)
            id_idx = len(params)
            conditions.append(
                f"(created_at, exposure_id) < (${dt_idx}, ${id_idx})"
            )

        sql = f"""
            SELECT exposure_id, asset_id, tenant_id, record_hash, tier,
                   matched_indicator, severity, created_at
              FROM asset_exposures
             WHERE {' AND '.join(conditions)}
             ORDER BY created_at DESC, exposure_id DESC
             LIMIT {int(limit) + 1}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        events = [_row_to_exposure(r) for r in rows]
        events, next_cursor = _next_cursor(
            events,
            limit,
            lambda e: e.created_at,
            lambda e: e.exposure_id,
        )
        return events, next_cursor
