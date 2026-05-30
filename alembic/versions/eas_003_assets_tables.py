"""Create assets, asset_exposures, and exposure_alert_deliveries tables.

Creates the three tables that back Asset Exposure Monitoring per Design §4.10
and §10.2: ``assets`` (tenant-scoped registered assets), ``asset_exposures``
(match events joining assets to normalized records), and
``exposure_alert_deliveries`` (audit log for downstream alert routing).

Indexes are intentionally deferred to ``eas_004_asset_exposures_indexes`` —
only table DDL, CHECK constraints, defaults, and foreign keys belong here.

Revision ID: eas_003
Revises: eas_002
Create Date: 2026-05-11
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "eas_003"
down_revision = "eas_002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # assets: tenant-scoped registered assets (R1.1, R24.1).
    op.create_table(
        "assets",
        sa.Column(
            "asset_id",
            postgresql.UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("tenant_id", postgresql.UUID, nullable=False),
        sa.Column("asset_type", sa.Text, nullable=False),
        sa.Column("normalized_value", sa.Text, nullable=False),
        sa.Column("raw_value", sa.Text, nullable=False),
        sa.Column(
            "is_active",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("TRUE"),
        ),
        sa.Column(
            "capture_screenshots",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("FALSE"),
        ),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "asset_type IN ('ip','cidr','domain','asn','hostname')",
            name="chk_assets_asset_type",
        ),
    )

    # asset_exposures: match events. tenant_id is denormalized from the parent
    # asset to support tenant-scoped listings without a join (Design §4.10).
    op.create_table(
        "asset_exposures",
        sa.Column(
            "exposure_id",
            postgresql.UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "asset_id",
            postgresql.UUID,
            sa.ForeignKey("assets.asset_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID, nullable=False),
        sa.Column("record_hash", sa.Text, nullable=False),
        sa.Column("tier", sa.SmallInteger, nullable=False),
        sa.Column("matched_indicator", sa.Text, nullable=False),
        sa.Column("severity", sa.Text, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint("tier >= 1 AND tier <= 29", name="chk_ae_tier"),
        sa.CheckConstraint(
            "severity IN ('low','medium','high','critical')",
            name="chk_ae_severity",
        ),
    )

    # exposure_alert_deliveries: audit trail for Alertmanager / tenant-webhook
    # fan-out. CASCADE on exposure deletion so the audit row never outlives
    # the exposure it references (Design §4.10).
    op.create_table(
        "exposure_alert_deliveries",
        sa.Column(
            "delivery_id",
            postgresql.UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "exposure_id",
            postgresql.UUID,
            sa.ForeignKey("asset_exposures.exposure_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("receiver", sa.Text, nullable=False),
        sa.Column(
            "delivered_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("status", sa.Text, nullable=False),
        sa.CheckConstraint(
            "status IN ('sent','failed','buffered')",
            name="chk_ead_status",
        ),
    )


def downgrade() -> None:
    # Reverse dependency order: exposure_alert_deliveries depends on
    # asset_exposures, which depends on assets.
    op.drop_table("exposure_alert_deliveries")
    op.drop_table("asset_exposures")
    op.drop_table("assets")
