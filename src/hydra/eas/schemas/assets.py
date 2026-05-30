"""Asset and exposure request/response schemas (Design §4.2).

Implements the public contract for Capability 1 — Asset Exposure Monitoring:
AssetType / ExposureSeverity enums, AssetCreate (with per-asset-type value
validation), AssetResponse, and ExposureResponse. The `_validate_by_type`
model-validator on AssetCreate delivers R1.2 (422 VALIDATION_ERROR on malformed
input) and matches the RFC 1035 domain/hostname regex and ASN 32-bit range from
Design §3.2 / §4.2.
"""

from __future__ import annotations

import ipaddress
import re
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

__all__ = [
    "AssetType",
    "ExposureSeverity",
    "AssetCreate",
    "AssetResponse",
    "ExposureResponse",
]


# RFC 1035 hostname / domain — labels 1..63 chars, total <= 253
_DOMAIN_RE = re.compile(
    r"^(?=.{1,253}$)(?!-)([A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$"
)


class AssetType(str, Enum):
    IP = "ip"
    CIDR = "cidr"
    DOMAIN = "domain"
    ASN = "asn"
    HOSTNAME = "hostname"


class ExposureSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AssetCreate(BaseModel):
    """POST /api/v1/assets request body."""

    asset_type: AssetType
    value: str = Field(..., min_length=1, max_length=253, examples=["192.0.2.0/24"])
    capture_screenshots: bool = Field(
        default=False,
        description="When True, automatic screenshot captures fire on exposure.",
    )
    notes: str | None = Field(default=None, max_length=1024)

    @model_validator(mode="after")
    def _validate_by_type(self) -> "AssetCreate":
        v = self.value.strip()
        if self.asset_type is AssetType.IP:
            ipaddress.ip_address(v)  # raises ValueError if bad
        elif self.asset_type is AssetType.CIDR:
            ipaddress.ip_network(v, strict=False)
        elif self.asset_type is AssetType.DOMAIN:
            if not _DOMAIN_RE.match(v):
                raise ValueError("domain must be RFC 1035 compliant")
        elif self.asset_type is AssetType.HOSTNAME:
            if not _DOMAIN_RE.match(v):
                raise ValueError("hostname must be RFC 1035 compliant")
        elif self.asset_type is AssetType.ASN:
            stripped = v.removeprefix("AS").removeprefix("as")
            if not stripped.isdigit() or not (0 <= int(stripped) <= 4_294_967_295):
                raise ValueError("asn must be a 32-bit unsigned integer")
        return self


class AssetResponse(BaseModel):
    asset_id: UUID
    tenant_id: UUID
    asset_type: AssetType
    value: str
    normalized_value: str
    is_active: bool
    capture_screenshots: bool
    notes: str | None = None
    created_at: datetime
    deactivated_at: datetime | None = None


class ExposureResponse(BaseModel):
    exposure_id: UUID
    asset_id: UUID
    record_hash: str = Field(..., pattern=r"^[0-9a-f]{16}$")
    tier: int = Field(..., ge=1, le=29)
    matched_indicator: str
    severity: ExposureSeverity
    created_at: datetime
    record_preview: dict[str, Any] | None = Field(
        default=None,
        description="Non-sensitive subset of the source record payload.",
    )
