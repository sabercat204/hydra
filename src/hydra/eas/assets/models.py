"""Internal dataclasses for Capability 1 — Asset Exposure Monitoring (Design §4.9).

These are intentionally separate from the Pydantic schemas in
``hydra.eas.schemas.assets``: the API contract can evolve independently from
the internal representation used by the monitor, repository, and alerter.

Three immutable (``frozen=True``, ``slots=True``) dataclasses:

* :class:`Asset` — a row from the ``assets`` PG table.
* :class:`ExposureEvent` — a row from ``asset_exposures``.
* :class:`AssetMatch` — an in-memory pairing of an :class:`Asset` with the
  indicator string that matched it plus a short ``match_reason`` tag for
  logging and evidence (values like ``"ip_exact"``, ``"cidr_contains"``,
  ``"domain_suffix"``, ``"hostname_exact"``, ``"asn_equals"``).

``frozen=True`` is appropriate here: the monitor's hot path passes these
through extractor / matcher / repository coroutines and should not mutate
them in place.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

__all__ = ["Asset", "ExposureEvent", "AssetMatch"]


@dataclass(slots=True, frozen=True)
class Asset:
    """Tenant-owned monitored asset (Design §4.9).

    ``asset_type`` is stored as a plain string rather than the
    :class:`hydra.eas.schemas.assets.AssetType` enum so that repository
    code can hand rows from ``asyncpg`` straight into this dataclass with
    no coercion step.
    """

    asset_id: UUID
    tenant_id: UUID
    asset_type: str
    normalized_value: str
    raw_value: str
    is_active: bool
    capture_screenshots: bool
    created_at: datetime
    deactivated_at: datetime | None
    notes: str | None = None


@dataclass(slots=True, frozen=True)
class ExposureEvent:
    """A persisted match between an indicator in a NormalizedRecord and an asset.

    ``tenant_id`` is denormalized from the parent asset so that tenant-scoped
    listings can be served without a join (Design §4.10).
    """

    exposure_id: UUID
    asset_id: UUID
    tenant_id: UUID
    record_hash: str
    tier: int
    matched_indicator: str
    severity: str
    created_at: datetime


@dataclass(slots=True, frozen=True)
class AssetMatch:
    """An in-memory pairing of an asset with a matched indicator.

    Produced by :class:`hydra.eas.assets.monitor.AssetMonitor` and consumed by
    :class:`hydra.eas.assets.alerter.ExposureAlerter`. ``match_reason`` is a
    short tag describing which branch of the matcher fired so that alerts and
    audit rows carry human-readable evidence without recomputing the match.
    """

    asset: Asset
    matched_indicator: str
    match_reason: str
