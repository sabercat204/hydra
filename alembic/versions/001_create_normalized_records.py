"""Create normalized_records table with PostGIS support.

Revision ID: 001
Revises:
Create Date: 2026-04-03
"""

from alembic import op
import sqlalchemy as sa

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable PostGIS extension
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    op.create_table(
        "normalized_records",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("stream_id", sa.Text, nullable=False),
        sa.Column("tier", sa.SmallInteger, nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("geo", sa.Column.__class__, nullable=True),  # PostGIS handled via raw SQL
        sa.Column("payload", sa.JSON, nullable=False),
        sa.Column("source_name", sa.Text, nullable=False),
        sa.Column("source_url", sa.Text, nullable=True),
        sa.Column("adapter_type", sa.Text, nullable=False),
        sa.Column("access_level", sa.Text, server_default="green"),
        sa.Column("raw_hash", sa.Text, nullable=False, unique=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("confidence", sa.Float, server_default="1.0"),
        sa.Column("tags", sa.ARRAY(sa.Text), server_default="{}"),
        sa.Column("storage_status", sa.Text, server_default="pending"),
        sa.Column("storage_engines", sa.ARRAY(sa.Text), server_default="{}"),
        sa.CheckConstraint("confidence >= 0.0 AND confidence <= 1.0", name="chk_confidence"),
        sa.CheckConstraint("tier >= 1 AND tier <= 28", name="chk_tier"),
    )

    # The geo column needs to be added via raw SQL for PostGIS geometry type
    op.execute("""
        ALTER TABLE normalized_records
        DROP COLUMN IF EXISTS geo;
    """)
    op.execute("""
        ALTER TABLE normalized_records
        ADD COLUMN geo geometry(Geometry, 4326);
    """)

    # Create indexes
    op.create_index("idx_nr_stream_id", "normalized_records", ["stream_id"])
    op.create_index("idx_nr_tier", "normalized_records", ["tier"])
    op.create_index("idx_nr_timestamp", "normalized_records", ["timestamp"])
    op.create_index("idx_nr_raw_hash", "normalized_records", ["raw_hash"])
    op.create_index("idx_nr_ingested_at", "normalized_records", ["ingested_at"])

    # GIN indexes
    op.execute("CREATE INDEX idx_nr_tags ON normalized_records USING GIN (tags)")
    op.execute("CREATE INDEX idx_nr_payload ON normalized_records USING GIN (payload jsonb_path_ops)")

    # Spatial index
    op.execute("CREATE INDEX idx_nr_geo ON normalized_records USING GIST (geo) WHERE geo IS NOT NULL")

    # Partial index for storage_status
    op.execute(
        "CREATE INDEX idx_nr_storage_status ON normalized_records (storage_status) "
        "WHERE storage_status != 'complete'"
    )


def downgrade() -> None:
    op.drop_table("normalized_records")
