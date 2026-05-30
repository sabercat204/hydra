"""Add tenant_id column to api_keys and backfill per Design §3.1.

Three-step backfill:
    1. ``ADD COLUMN tenant_id UUID NULL``.
    2. Data-migration pass assigning one fresh UUID4 per distinct ``name``
       prefix (treating ``name`` as ``{tenant_slug}-{keyname}``). Keys whose
       ``name`` has no hyphen each get their own fresh UUID4. If an operator
       pre-populated the ``api_key_tenant_backfill`` side-table by running
       ``scripts/backfill_api_keys_tenant.py`` first, that explicit mapping
       takes precedence.
    3. ``ALTER COLUMN tenant_id SET NOT NULL``.
    4. ``CREATE INDEX idx_api_keys_tenant ON api_keys(tenant_id)``.

Revision ID: eas_002
Revises: eas_001
Create Date: 2026-05-11
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "eas_002"
down_revision = "eas_001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # (1) Add the column as nullable so the existing rows can be backfilled.
    op.add_column(
        "api_keys",
        sa.Column("tenant_id", postgresql.UUID, nullable=True),
    )

    # (2) Data-migration pass. Per Design §3.1, one fresh UUID4 per distinct
    # `name` prefix (or per key when the name does not follow
    # `{tenant_slug}-{keyname}`).
    #
    # Operators who want to merge keys into existing tenants MUST run
    # `scripts/backfill_api_keys_tenant.py` *before* applying this migration
    # in production, writing a side-table `api_key_tenant_backfill(key_id,
    # tenant_id)`. If that table is absent the migration is a no-op on that
    # side-table and falls through to the prefix-based backfill below.
    bind = op.get_bind()
    backfill_exists = bind.execute(
        sa.text("SELECT to_regclass('api_key_tenant_backfill')")
    ).scalar()
    if backfill_exists is not None:
        op.execute(
            sa.text(
                """
                UPDATE api_keys k
                   SET tenant_id = b.tenant_id
                  FROM api_key_tenant_backfill b
                 WHERE b.key_id = k.key_id
                """
            )
        )

    # Prefix-based backfill: assign one UUID per distinct
    # `split_part(name, '-', 1)` group, for keys whose name contains at least
    # one hyphen and were not already filled in from the side-table.
    op.execute(
        sa.text(
            """
            WITH prefix_assignments AS (
                SELECT DISTINCT split_part(name, '-', 1) AS prefix,
                       gen_random_uuid() AS tenant_id
                  FROM api_keys
                 WHERE tenant_id IS NULL
                   AND position('-' IN name) > 0
            )
            UPDATE api_keys k
               SET tenant_id = pa.tenant_id
              FROM prefix_assignments pa
             WHERE split_part(k.name, '-', 1) = pa.prefix
               AND k.tenant_id IS NULL
            """
        )
    )

    # For keys without a hyphen prefix, generate a fresh UUID per key.
    op.execute(
        sa.text(
            """
            UPDATE api_keys
               SET tenant_id = gen_random_uuid()
             WHERE tenant_id IS NULL
            """
        )
    )

    # (3) Every row now has a tenant_id; enforce NOT NULL.
    op.alter_column("api_keys", "tenant_id", nullable=False)

    # (4) Index for tenant-scoped lookups.
    op.create_index("idx_api_keys_tenant", "api_keys", ["tenant_id"])


def downgrade() -> None:
    op.drop_index("idx_api_keys_tenant", table_name="api_keys")
    op.drop_column("api_keys", "tenant_id")
