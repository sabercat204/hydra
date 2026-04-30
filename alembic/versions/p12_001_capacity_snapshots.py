"""Create capacity_snapshots table for P12 capacity planning.

Revision ID: p12_001
Revises: p11_002
Create Date: 2026-04-29

Rationale (design.md §"Component 7: CapacityPlanner"):
The capacity planner collects periodic size snapshots per storage
engine and runs a linear regression over the last 7 days to project
days-to-threshold. Persisting snapshots to PostgreSQL makes the
projection stable across API restarts and allows dashboards to render
historical growth trends without relying solely on Prometheus
retention.

Schema:
  id           SERIAL PRIMARY KEY
  engine       VARCHAR(32) NOT NULL  -- postgres, elasticsearch, influxdb, minio
  metric_name  VARCHAR(64) NOT NULL  -- e.g. 'pg_database_size', 'es_index:hydra-records'
  value_bytes  BIGINT NOT NULL       -- non-negative size in bytes
  collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()

Constraints:
  - engine ∈ {postgres, elasticsearch, influxdb, minio} via CHECK
  - value_bytes ≥ 0 via CHECK

Indexes:
  - (engine, collected_at DESC)  for time-window queries used by the
    growth-rate regression

Requirements: 24.1, 24.2, 24.3, 24.4.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "p12_001"
down_revision = "p11_002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "capacity_snapshots",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("engine", sa.String(length=32), nullable=False),
        sa.Column("metric_name", sa.String(length=64), nullable=False),
        sa.Column("value_bytes", sa.BigInteger, nullable=False),
        sa.Column(
            "collected_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "engine IN ('postgres', 'elasticsearch', 'influxdb', 'minio')",
            name="ck_capacity_snapshots_engine",
        ),
        sa.CheckConstraint(
            "value_bytes >= 0",
            name="ck_capacity_snapshots_value_non_negative",
        ),
    )
    op.create_index(
        "idx_capacity_snapshots_engine_time",
        "capacity_snapshots",
        ["engine", sa.text("collected_at DESC")],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_capacity_snapshots_engine_time",
        table_name="capacity_snapshots",
    )
    op.drop_table("capacity_snapshots")
