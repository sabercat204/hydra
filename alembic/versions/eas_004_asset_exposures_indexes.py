"""Create indexes for assets, asset_exposures, and exposure_alert_deliveries.

Creates the seven indexes specified by Design §4.10 for the three tables
introduced in ``eas_003_assets_tables``. Raw ``CREATE INDEX`` SQL is used
because Alembic's ``op.create_index`` has awkward support for partial
indexes with ``WHERE`` clauses and ``DESC`` column ordering.

Indexes created (in order):
    1. ``idx_assets_tenant_type_value_active`` — partial unique on
       ``assets(tenant_id, asset_type, normalized_value) WHERE is_active``.
       Enforces R1.3 registration idempotency.
    2. ``idx_assets_tenant`` — standard on ``assets(tenant_id)``.
    3. ``idx_assets_type_value`` — partial on
       ``assets(asset_type, normalized_value) WHERE is_active``. Supports
       the ``AssetMonitor.list_matching`` lookup path.
    4. ``idx_asset_exposures_asset_created`` — standard on
       ``asset_exposures(asset_id, created_at DESC)``. R24.3.
    5. ``idx_asset_exposures_dedup`` — partial unique on
       ``asset_exposures(asset_id, record_hash, matched_indicator)``.
       R24.3 / R3.3 dedup.
    6. ``idx_asset_exposures_tenant_created`` — standard on
       ``asset_exposures(tenant_id, created_at DESC)``. Supports the
       ``/exposures`` cross-asset listing path.
    7. ``idx_exposure_alert_deliveries_exposure`` — standard on
       ``exposure_alert_deliveries(exposure_id)``.

Revision ID: eas_004
Revises: eas_003
Create Date: 2026-05-11
"""

from alembic import op

revision = "eas_004"
down_revision = "eas_003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # (1) assets: partial unique index enforcing R1.3 — at most one active
    # row per (tenant_id, asset_type, normalized_value). Deactivated rows
    # are excluded so a tenant can re-register a previously deactivated
    # asset without a unique-violation conflict.
    op.execute(
        """
        CREATE UNIQUE INDEX idx_assets_tenant_type_value_active
            ON assets(tenant_id, asset_type, normalized_value)
            WHERE is_active = TRUE
        """
    )

    # (2) assets: tenant-scoped list lookups.
    op.execute(
        """
        CREATE INDEX idx_assets_tenant ON assets(tenant_id)
        """
    )

    # (3) assets: supports AssetMonitor.list_matching — lookups by
    # (asset_type, normalized_value) across all tenants, restricted to
    # active rows.
    op.execute(
        """
        CREATE INDEX idx_assets_type_value
            ON assets(asset_type, normalized_value)
            WHERE is_active = TRUE
        """
    )

    # (4) asset_exposures: per-asset listings ordered by newest first
    # (R24.3). DESC ordering matches the router's default sort.
    op.execute(
        """
        CREATE INDEX idx_asset_exposures_asset_created
            ON asset_exposures(asset_id, created_at DESC)
        """
    )

    # (5) asset_exposures: dedup enforcement (R24.3 / R3.3). Paired with
    # the ``ON CONFLICT DO NOTHING`` clause in ExposureRepository.insert.
    op.execute(
        """
        CREATE UNIQUE INDEX idx_asset_exposures_dedup
            ON asset_exposures(asset_id, record_hash, matched_indicator)
        """
    )

    # (6) asset_exposures: cross-asset tenant listings for GET /exposures.
    op.execute(
        """
        CREATE INDEX idx_asset_exposures_tenant_created
            ON asset_exposures(tenant_id, created_at DESC)
        """
    )

    # (7) exposure_alert_deliveries: per-exposure audit-trail lookups.
    op.execute(
        """
        CREATE INDEX idx_exposure_alert_deliveries_exposure
            ON exposure_alert_deliveries(exposure_id)
        """
    )


def downgrade() -> None:
    # Reverse order of creation. ``IF EXISTS`` is used for safety so that
    # a partial upgrade followed by a downgrade does not fail on missing
    # indexes.
    op.execute("DROP INDEX IF EXISTS idx_exposure_alert_deliveries_exposure")
    op.execute("DROP INDEX IF EXISTS idx_asset_exposures_tenant_created")
    op.execute("DROP INDEX IF EXISTS idx_asset_exposures_dedup")
    op.execute("DROP INDEX IF EXISTS idx_asset_exposures_asset_created")
    op.execute("DROP INDEX IF EXISTS idx_assets_type_value")
    op.execute("DROP INDEX IF EXISTS idx_assets_tenant")
    op.execute("DROP INDEX IF EXISTS idx_assets_tenant_type_value_active")
