"""Integration test for the EAS migration upgrade → downgrade → upgrade cycle.

This test exercises the four EAS migrations (``eas_001`` through ``eas_004``)
end-to-end against a disposable PostgreSQL instance and verifies that all
tables, columns, CHECK constraints, and indexes specified in tasks 2.1–2.4
are present after a full upgrade, absent after a downgrade to ``eas_001``,
and restored after a re-upgrade.

Because the CI environment used by most contributors does not ship with a
throwaway PG instance, the test is gated on the ``HYDRA_TEST_POSTGRES_DSN``
environment variable. When the variable is unset or the DSN is unreachable,
the test is skipped with a message explaining how to enable it. This keeps
the suite green on laptop installs while still exercising real migration
behavior in environments that provide a test database (docker-compose CI,
developer workflows with ``pg_tmp``, etc.).

How to run locally:

    docker run --rm -e POSTGRES_PASSWORD=hydra -p 55432:5432 \\
        -d postgres:16-alpine
    export HYDRA_TEST_POSTGRES_DSN=\
        "postgresql+psycopg2://postgres:hydra@localhost:55432/postgres"
    pytest tests/eas/test_migrations.py -v

Covers: R24.1, R24.2, R24.3, R24.4.
"""

from __future__ import annotations

import os
from typing import Iterator

import pytest

# ---------------------------------------------------------------------------
# Module-level skip gate
# ---------------------------------------------------------------------------
#
# Mark every test in this module as ``integration`` and short-circuit the
# collection phase entirely when no test DSN is provided. We do this at module
# scope (via ``allow_module_level=True``) so pytest does not pay the import
# cost of alembic/sqlalchemy on environments that cannot exercise them.

pytestmark = pytest.mark.integration

_DSN_ENV_VAR = "HYDRA_TEST_POSTGRES_DSN"
_DSN_RAW = os.getenv(_DSN_ENV_VAR)

if not _DSN_RAW:
    pytest.skip(
        f"{_DSN_ENV_VAR} not set — skipping EAS migration integration test. "
        "Set it to a disposable PostgreSQL DSN to enable this test "
        "(e.g. 'postgresql+psycopg2://postgres:pass@localhost:5432/postgres').",
        allow_module_level=True,
    )

# Deferred imports — only reached when the DSN is present.
import alembic.command  # noqa: E402
import alembic.config  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402
from sqlalchemy.exc import OperationalError  # noqa: E402


# ---------------------------------------------------------------------------
# DSN normalization
# ---------------------------------------------------------------------------
#
# Alembic's ``env.py`` uses an async engine (``async_engine_from_config``) and
# reads the DSN from ``hydra.config.settings.database.postgres_dsn``, which
# requires the ``+asyncpg`` driver marker. Our introspection queries, however,
# need a synchronous engine. We therefore derive both forms from whatever the
# operator provided in ``HYDRA_TEST_POSTGRES_DSN``.


def _to_sync_dsn(dsn: str) -> str:
    """Return a DSN suitable for a synchronous SQLAlchemy engine.

    Strips the ``+asyncpg`` driver marker if present; leaves explicit sync
    drivers (``+psycopg2``, ``+psycopg``) untouched; falls back to the bare
    ``postgresql://`` scheme when no driver marker is set (which lets
    SQLAlchemy pick whatever psycopg variant is installed).
    """
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


def _to_async_dsn(dsn: str) -> str:
    """Return a DSN suitable for alembic's async ``env.py``.

    Adds the ``+asyncpg`` driver marker when the operator provided a plain
    ``postgresql://`` DSN; converts ``+psycopg2``/``+psycopg`` to ``+asyncpg``
    because alembic's ``async_engine_from_config`` requires an async driver.
    """
    for sync_prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
        if dsn.startswith(sync_prefix):
            return "postgresql+asyncpg://" + dsn[len(sync_prefix) :]
    if dsn.startswith("postgresql://"):
        return "postgresql+asyncpg://" + dsn[len("postgresql://") :]
    return dsn


_SYNC_DSN = _to_sync_dsn(_DSN_RAW)
_ASYNC_DSN = _to_async_dsn(_DSN_RAW)


# ---------------------------------------------------------------------------
# Expected catalog objects
# ---------------------------------------------------------------------------
#
# These lists mirror tasks 2.3 and 2.4 and are the single source of truth for
# what the test expects to see after a full upgrade. Update them only when the
# migration set itself changes.

_EAS_TABLES = ("assets", "asset_exposures", "exposure_alert_deliveries")

_EAS_INDEXES = (
    # From eas_004 — Design §4.10, satisfies R24.2 / R24.3.
    "idx_assets_tenant_type_value_active",
    "idx_assets_tenant",
    "idx_assets_type_value",
    "idx_asset_exposures_asset_created",
    "idx_asset_exposures_dedup",
    "idx_asset_exposures_tenant_created",
    "idx_exposure_alert_deliveries_exposure",
)


# ---------------------------------------------------------------------------
# Introspection helpers
# ---------------------------------------------------------------------------


def _has_table(conn, table_name: str) -> bool:
    """Return True iff ``table_name`` exists in the ``public`` schema."""
    row = conn.execute(
        text(
            """
            SELECT 1
              FROM information_schema.tables
             WHERE table_schema = 'public'
               AND table_name = :name
            """
        ),
        {"name": table_name},
    ).first()
    return row is not None


def _has_column(conn, table_name: str, column_name: str) -> bool:
    """Return True iff the given column exists on the given public table."""
    row = conn.execute(
        text(
            """
            SELECT 1
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = :table
               AND column_name = :column
            """
        ),
        {"table": table_name, "column": column_name},
    ).first()
    return row is not None


def _has_index(conn, index_name: str) -> bool:
    """Return True iff ``index_name`` exists in the ``public`` schema."""
    row = conn.execute(
        text(
            """
            SELECT 1
              FROM pg_indexes
             WHERE schemaname = 'public'
               AND indexname = :name
            """
        ),
        {"name": index_name},
    ).first()
    return row is not None


def _column_is_uuid_not_null(conn, table_name: str, column_name: str) -> bool:
    """Return True iff the column is a ``uuid NOT NULL``."""
    row = conn.execute(
        text(
            """
            SELECT data_type, is_nullable
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = :table
               AND column_name = :column
            """
        ),
        {"table": table_name, "column": column_name},
    ).first()
    if row is None:
        return False
    data_type, is_nullable = row[0], row[1]
    return data_type == "uuid" and is_nullable == "NO"


def _tier_check_accepts(conn, tier: int) -> bool:
    """Probe the ``chk_tier`` CHECK constraint by attempting an insert.

    A real INSERT is the most reliable way to verify that the CHECK allows a
    given value — parsing ``pg_constraint.consrc`` is fragile across PG
    versions and non-trivial to do robustly. We wrap the probe in a
    ``SAVEPOINT`` so that a constraint violation does not poison the
    surrounding transaction.
    """
    # Probe inside a nested transaction so any CHECK failure is localized.
    savepoint = conn.begin_nested()
    try:
        conn.execute(
            text(
                """
                INSERT INTO normalized_records (record_hash, tier)
                VALUES (:hash, :tier)
                """
            ),
            {"hash": f"test-tier-probe-{tier}", "tier": tier},
        )
    except Exception:  # noqa: BLE001 — any failure means the tier is rejected
        savepoint.rollback()
        return False
    # Roll back the probe so the row is not persisted either way; we are only
    # interested in whether the INSERT would be allowed.
    savepoint.rollback()
    return True


# ---------------------------------------------------------------------------
# Alembic configuration
# ---------------------------------------------------------------------------
#
# ``alembic/env.py`` unconditionally overrides ``sqlalchemy.url`` with
# ``hydra.config.settings.database.postgres_dsn``. To redirect alembic at the
# disposable test instance we therefore have to mutate the settings object
# itself before every command invocation. ``_make_alembic_config`` also sets
# ``sqlalchemy.url`` on the returned Config — harmless but belt-and-suspenders.


def _make_alembic_config() -> alembic.config.Config:
    cfg = alembic.config.Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", _ASYNC_DSN)
    return cfg


def _override_hydra_settings_dsn() -> None:
    """Point ``hydra.config.settings`` at the test DSN.

    Safe to call repeatedly. The mutation leaks into the running process but
    that is fine here — this module is only ever loaded when a test DSN is
    already in effect, so anything else that imports ``settings`` during the
    test will get the right DSN too.
    """
    # Import lazily so the skip path above does not pay this cost.
    from hydra.config import settings

    settings.database.postgres_dsn = _ASYNC_DSN


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def engine() -> Iterator[Engine]:
    """Yield a sync SQLAlchemy engine against the test DSN.

    Skips the whole module if the DSN is unreachable, using pytest's
    ``pytest.skip`` so the failure reads as "infrastructure not available"
    rather than a red test. On teardown, always reset the database by running
    ``alembic downgrade base`` so that subsequent test runs (and parallel
    workers) start from a clean slate.
    """
    try:
        eng = create_engine(_SYNC_DSN, future=True)
        # Force a connection attempt; OperationalError here means the DSN is
        # set but no server is listening (typical CI-without-docker case).
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as exc:
        pytest.skip(
            f"Could not connect to {_DSN_ENV_VAR}={_SYNC_DSN!r}: {exc}. "
            "Start a disposable PostgreSQL instance (see the module "
            "docstring) or unset the variable to skip this test."
        )
    except ModuleNotFoundError as exc:
        # Raised by SQLAlchemy when the selected driver (psycopg2, psycopg) is
        # not installed. Treat as "infrastructure not available" so the test
        # skips cleanly on laptops that happen to have the DSN set but no
        # driver.
        pytest.skip(
            f"Sync PostgreSQL driver unavailable for {_SYNC_DSN!r}: {exc}. "
            f"Install psycopg2-binary or psycopg to enable this test."
        )

    _override_hydra_settings_dsn()

    try:
        yield eng
    finally:
        # Best-effort cleanup: roll every migration all the way back so the
        # test instance is empty for the next run. Swallow errors so a failure
        # during the test itself does not mask its traceback.
        try:
            alembic.command.downgrade(_make_alembic_config(), "base")
        except Exception:  # noqa: BLE001 — teardown is best-effort only
            pass
        eng.dispose()


# ---------------------------------------------------------------------------
# Phase assertions
# ---------------------------------------------------------------------------
#
# The three phases of the cycle share most of their assertions, so they are
# factored into helper routines. Each helper opens a short transaction and
# asserts a coherent view of the catalog.


def _assert_fully_upgraded(engine: Engine) -> None:
    """Assert the catalog state after ``alembic upgrade head``."""
    with engine.connect() as conn:
        # (R24.1) All three EAS tables exist.
        for table in _EAS_TABLES:
            assert _has_table(conn, table), f"expected table {table!r} to exist after upgrade"

        # (R24.4) api_keys.tenant_id is UUID NOT NULL (eas_002).
        assert _has_column(conn, "api_keys", "tenant_id"), (
            "expected column api_keys.tenant_id to exist after upgrade"
        )
        assert _column_is_uuid_not_null(conn, "api_keys", "tenant_id"), (
            "expected api_keys.tenant_id to be UUID NOT NULL after upgrade"
        )

        # (R9.1 / R24.1) The relaxed chk_tier allows tier 29 (eas_001).
        with conn.begin():
            assert _tier_check_accepts(conn, 29), (
                "expected chk_tier to allow tier 29 after eas_001 upgrade"
            )

        # (R24.2 / R24.3) All seven EAS indexes exist.
        for index in _EAS_INDEXES:
            assert _has_index(conn, index), (
                f"expected index {index!r} to exist after upgrade"
            )


def _assert_downgraded_to_eas_001(engine: Engine) -> None:
    """Assert the catalog state after rolling back to ``eas_001``.

    At this point only ``eas_001`` (the tier-relaxation migration) is applied;
    ``eas_002`` (tenant_id), ``eas_003`` (tables), and ``eas_004`` (indexes)
    have all been reverted.
    """
    with engine.connect() as conn:
        # Tables introduced by eas_003 must be gone.
        for table in _EAS_TABLES:
            assert not _has_table(conn, table), (
                f"expected table {table!r} to be absent after downgrade to eas_001"
            )

        # Column introduced by eas_002 must be gone.
        assert not _has_column(conn, "api_keys", "tenant_id"), (
            "expected column api_keys.tenant_id to be absent after downgrade to eas_001"
        )

        # Indexes created by eas_004 must be gone. Indexes attached to dropped
        # tables are removed automatically by PG, but we assert explicitly for
        # clarity and to catch any accidental cross-table index.
        for index in _EAS_INDEXES:
            assert not _has_index(conn, index), (
                f"expected index {index!r} to be absent after downgrade to eas_001"
            )


# ---------------------------------------------------------------------------
# The test itself
# ---------------------------------------------------------------------------


def test_eas_migrations_upgrade_downgrade_upgrade_cycle(engine: Engine) -> None:
    """Full round-trip: upgrade head → downgrade eas_001 → upgrade head.

    Satisfies R24.1–R24.4 by asserting the same invariants at the start
    (post-upgrade), middle (post-downgrade), and end (post-re-upgrade) of
    the cycle. A regression in any of the four EAS migrations — table DDL,
    column additions, CHECK relaxations, or index creation — will show up
    as a failed assertion in one of the three phases.
    """
    cfg = _make_alembic_config()

    # Establish a clean baseline before the first upgrade. ``downgrade base``
    # is a no-op when nothing has been applied yet, so this is also safe on
    # a freshly-created test database.
    alembic.command.downgrade(cfg, "base")

    # Phase 1: full upgrade.
    alembic.command.upgrade(cfg, "head")
    _assert_fully_upgraded(engine)

    # Phase 2: downgrade to eas_001 (rolls back eas_002, eas_003, eas_004).
    alembic.command.downgrade(cfg, "eas_001")
    _assert_downgraded_to_eas_001(engine)

    # Phase 3: re-upgrade to head. Must restore the exact same catalog state
    # we saw in Phase 1 — this is the core reversibility property from R24.1.
    alembic.command.upgrade(cfg, "head")
    _assert_fully_upgraded(engine)
