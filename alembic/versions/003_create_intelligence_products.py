"""Create intelligence_products table.

Revision ID: 003
Revises: 002
Create Date: 2026-04-04
"""

from alembic import op
import sqlalchemy as sa

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "intelligence_products",
        sa.Column(
            "product_id",
            sa.dialects.postgresql.UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("product_type", sa.Text, nullable=False),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("classification", sa.Text, nullable=False, server_default="green"),
        sa.Column(
            "generated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("time_window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("time_window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sections", sa.JSON, nullable=False),
        sa.Column("summary", sa.Text, nullable=False),
        sa.Column("key_findings", sa.ARRAY(sa.Text), server_default="{}"),
        sa.Column(
            "confidence_score",
            sa.Float,
            nullable=False,
        ),
        sa.Column(
            "completeness_score",
            sa.Float,
            nullable=False,
        ),
        sa.Column("source_tiers", sa.ARRAY(sa.Integer), server_default="{}"),
        sa.Column("record_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("correlation_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("parameters", sa.JSON, server_default="{}"),
        sa.Column("product_hash", sa.Text, nullable=False, unique=True),
        sa.Column("tags", sa.ARRAY(sa.Text), server_default="{}"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "confidence_score >= 0.0 AND confidence_score <= 1.0",
            name="chk_product_confidence",
        ),
        sa.CheckConstraint(
            "completeness_score >= 0.0 AND completeness_score <= 1.0",
            name="chk_product_completeness",
        ),
    )

    op.create_index("idx_product_type", "intelligence_products", ["product_type"])
    op.create_index(
        "idx_product_generated",
        "intelligence_products",
        ["generated_at"],
        postgresql_ops={"generated_at": "DESC"},
    )
    op.create_index(
        "idx_product_classification", "intelligence_products", ["classification"]
    )
    op.create_index(
        "idx_product_tiers",
        "intelligence_products",
        ["source_tiers"],
        postgresql_using="gin",
    )
    op.create_index(
        "idx_product_tags",
        "intelligence_products",
        ["tags"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_table("intelligence_products")
