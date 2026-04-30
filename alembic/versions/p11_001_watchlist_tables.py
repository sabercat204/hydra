"""Create watchlist tables — entity_watchlist and region_watchlist.

Revision ID: p11_001
Revises: 003
Create Date: 2026-04-04
"""

from alembic import op
import sqlalchemy as sa

revision = "p11_001"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "entity_watchlist",
        sa.Column("entity_id", sa.Text, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("entity_type", sa.Text, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("idx_entity_watchlist_type", "entity_watchlist", ["entity_type"])

    op.create_table(
        "region_watchlist",
        sa.Column("region_code", sa.Text, primary_key=True),
        sa.Column("name", sa.Text, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def downgrade() -> None:
    op.drop_table("region_watchlist")
    op.drop_table("entity_watchlist")
