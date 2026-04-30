"""Create api_keys table.

Revision ID: p11_002
Revises: p11_001
Create Date: 2026-04-04
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "p11_002"
down_revision = "p11_001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "api_keys",
        sa.Column(
            "key_id",
            postgresql.UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("key_hash", sa.Text, nullable=False, unique=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column(
            "scopes",
            sa.ARRAY(sa.Text),
            nullable=False,
            server_default=sa.text("'{read,search,write}'"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.text("TRUE")),
    )
    op.create_index("idx_api_keys_hash", "api_keys", ["key_hash"])
    op.create_index(
        "idx_api_keys_active",
        "api_keys",
        ["is_active"],
        postgresql_where=sa.text("is_active = TRUE"),
    )


def downgrade() -> None:
    op.drop_table("api_keys")
