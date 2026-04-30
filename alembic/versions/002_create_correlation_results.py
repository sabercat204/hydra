"""Create correlation_results table.

Revision ID: 002
Revises: 001
Create Date: 2026-04-04
"""

from alembic import op
import sqlalchemy as sa

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "correlation_results",
        sa.Column("correlation_id", sa.dialects.postgresql.UUID, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("pipeline_id", sa.Text, nullable=False),
        sa.Column("record_a_hash", sa.Text, nullable=False),
        sa.Column("record_b_hash", sa.Text, nullable=False),
        sa.Column("tier_a", sa.Integer, nullable=False),
        sa.Column("tier_b", sa.Integer, nullable=False),
        sa.Column("confidence", sa.Float, nullable=False),
        sa.Column("match_dimensions", sa.JSON, nullable=False),
        sa.Column("evidence", sa.JSON, nullable=False),
        sa.Column("correlation_hash", sa.Text, nullable=False, unique=True),
        sa.Column("tags", sa.ARRAY(sa.Text), server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["record_a_hash"], ["normalized_records.raw_hash"], name="fk_corr_record_a"),
        sa.ForeignKeyConstraint(["record_b_hash"], ["normalized_records.raw_hash"], name="fk_corr_record_b"),
        sa.CheckConstraint("confidence >= 0.0 AND confidence <= 1.0", name="chk_corr_confidence"),
    )

    op.create_index("idx_correlation_pipeline", "correlation_results", ["pipeline_id"])
    op.create_index("idx_correlation_tiers", "correlation_results", ["tier_a", "tier_b"])
    op.create_index("idx_correlation_confidence", "correlation_results", ["confidence"], postgresql_ops={"confidence": "DESC"})
    op.create_index("idx_correlation_record_a", "correlation_results", ["record_a_hash"])
    op.create_index("idx_correlation_record_b", "correlation_results", ["record_b_hash"])
    op.create_index("idx_correlation_created", "correlation_results", ["created_at"])


def downgrade() -> None:
    op.drop_table("correlation_results")
