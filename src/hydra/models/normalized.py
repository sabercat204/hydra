"""Universal data schema — the integration contract for all HYDRA components.

Every adapter produces NormalizedRecord. Every storage engine accepts it.
Every correlation pipeline consumes it.
"""

import re
import sys
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from enum import Enum

    class StrEnum(str, Enum):  # type: ignore[no-redef]
        """Backport of StrEnum for Python < 3.11."""


class Tier(IntEnum):
    """28 thematic data tiers."""

    GEOPHYSICAL_SEISMIC = 1
    ATMOSPHERIC_WEATHER = 2
    SPACE_WEATHER_SOLAR = 3
    SATELLITE_IMAGERY_EO = 4
    ECONOMIC_FINANCIAL = 5
    LAW_ENFORCEMENT = 6
    PUBLIC_HEALTH = 7
    INTERNATIONAL_ORGS = 8
    EU_EUROSTAT = 9
    ASIA_PACIFIC_GOV = 10
    AMERICAS_GOV_NON_US = 11
    ME_AFRICA_CENTRAL_ASIA_GOV = 12
    US_STATE_LOCAL = 13
    ARMS_DEFENSE_TRADE = 14
    CONFLICT_EVENT_DATA = 15
    CYBER_THREAT_INTEL = 16
    SOCIAL_MEDIA_WEB_OSINT = 17
    AVIATION_MARITIME = 18
    SANCTIONS_FINANCIAL_INTEL = 19
    NBC_THREAT = 20
    HUMAN_RIGHTS = 21
    ASTRONOMY_ASTROPHYSICS = 22
    SPACE_SITUATIONAL_AWARENESS = 23
    GEOSCIENCE_EARTH_SYSTEMS = 24
    ENVIRONMENTAL_CLIMATE = 25
    GLOBAL_HEALTH_EPI = 26
    ENERGY_INFRASTRUCTURE = 27
    NATIONAL_PORTAL_INDEX = 28


class AccessLevel(StrEnum):
    """HYDRA accessibility legend."""

    GREEN = "green"
    YELLOW = "yellow"
    BLUE = "blue"
    ORANGE = "orange"
    RED = "red"


GeoJSONType = Literal[
    "Point", "LineString", "Polygon",
    "MultiPoint", "MultiLineString", "MultiPolygon",
    "GeometryCollection",
]


class GeoPoint(BaseModel):
    """GeoJSON Point geometry."""

    type: Literal["Point"] = "Point"
    coordinates: List[float] = Field(
        ..., min_length=2, max_length=3,
        description="[lon, lat] or [lon, lat, alt]",
    )


class GeoGeometry(BaseModel):
    """GeoJSON geometry supporting all geometry types."""

    type: GeoJSONType
    coordinates: Optional[List[Any]] = None
    geometries: Optional[List["GeoGeometry"]] = None

    @field_validator("geometries", mode="before")
    @classmethod
    def _validate_geometries(cls, v: Any, info: Any) -> Any:
        if info.data.get("type") == "GeometryCollection" and v is None:
            raise ValueError("GeometryCollection requires 'geometries'")
        return v


class SourceMeta(BaseModel):
    """Metadata about the data source and fetch context."""

    source_name: str
    source_url: str = ""
    adapter_type: str
    access_level: str = AccessLevel.GREEN
    fetch_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    raw_format: str = ""
    api_version: str = ""
    rate_limit_remaining: Optional[int] = None


_HEX16_RE = re.compile(r"^[0-9a-f]{16}$")


class NormalizedRecord(BaseModel):
    """Universal record schema produced by all adapters."""

    stream_id: str
    tier: Tier
    timestamp: datetime
    geo: Optional[GeoGeometry] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    source_meta: SourceMeta
    raw_hash: str
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    tags: List[str] = Field(default_factory=list)

    @field_validator("timestamp", mode="before")
    @classmethod
    def _parse_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, str):
            v = datetime.fromisoformat(v)
        if isinstance(v, datetime) and v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v

    @field_validator("raw_hash")
    @classmethod
    def _validate_raw_hash(cls, v: str) -> str:
        if not _HEX16_RE.match(v):
            raise ValueError("raw_hash must be a 16-character lowercase hex string (xxhash64)")
        return v
