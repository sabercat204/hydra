"""Reusable matching strategies for correlation pipelines."""

from __future__ import annotations

import math
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from hydra.correlation.models import MatchScore
from hydra.models.normalized import NormalizedRecord

# ---------------------------------------------------------------------------
# Stopwords for keyword extraction (minimal English set)
# ---------------------------------------------------------------------------
_STOPWORDS: set[str] = {
    "the", "and", "for", "are", "but", "not", "you", "all", "can", "had",
    "her", "was", "one", "our", "out", "has", "have", "been", "from",
    "this", "that", "with", "they", "will", "each", "make", "like",
    "than", "them", "then", "what", "when", "where", "which", "their",
    "about", "would", "there", "could", "other", "into", "more", "some",
    "these", "also", "just", "over", "such", "only", "very", "after",
    "before", "between", "through", "during", "without", "again",
    "further", "once", "here", "both", "does", "doing", "being",
}


# ---------------------------------------------------------------------------
# Entity extraction configuration
# ---------------------------------------------------------------------------
@dataclass
class EntityExtractionConfig:
    """Declares how to extract entity identifiers from a tier's payload."""

    id_fields: list[str] = field(default_factory=list)
    name_fields: list[str] = field(default_factory=list)
    alias_fields: list[str] = field(default_factory=list)


ENTITY_EXTRACTION_MAP: dict[int, EntityExtractionConfig] = {
    16: EntityExtractionConfig(
        id_fields=["id", "mitre_attack_id", "cve_id"],
        name_fields=["name"],
        alias_fields=["aliases", "x_mitre_aliases"],
    ),
    19: EntityExtractionConfig(
        id_fields=["entity_id", "ofac_id", "lei"],
        name_fields=["name"],
        alias_fields=["aliases", "aka"],
    ),
    15: EntityExtractionConfig(
        id_fields=["event_id", "actor_id"],
        name_fields=["actor_name", "assoc_actor_name"],
        alias_fields=[],
    ),
    21: EntityExtractionConfig(
        id_fields=["case_id", "report_id"],
        name_fields=["subject_name", "perpetrator"],
        alias_fields=["aliases"],
    ),
    14: EntityExtractionConfig(
        id_fields=["program_id", "transfer_id"],
        name_fields=["supplier", "recipient"],
        alias_fields=[],
    ),
    8: EntityExtractionConfig(
        id_fields=["resolution_id", "event_id"],
        name_fields=["country", "organization"],
        alias_fields=[],
    ),
}

# Sub-region groupings (ISO 3166-1 alpha-2 → sub-region)
_SUBREGION_MAP: dict[str, str] = {
    # Eastern Europe
    "BY": "Eastern Europe", "BG": "Eastern Europe", "CZ": "Eastern Europe",
    "HU": "Eastern Europe", "MD": "Eastern Europe", "PL": "Eastern Europe",
    "RO": "Eastern Europe", "RU": "Eastern Europe", "SK": "Eastern Europe",
    "UA": "Eastern Europe",
    # Western Europe
    "AT": "Western Europe", "BE": "Western Europe", "FR": "Western Europe",
    "DE": "Western Europe", "LU": "Western Europe", "NL": "Western Europe",
    "CH": "Western Europe",
    # Middle East
    "IR": "Middle East", "IQ": "Middle East", "SY": "Middle East",
    "SA": "Middle East", "YE": "Middle East", "JO": "Middle East",
    "LB": "Middle East", "IL": "Middle East", "AE": "Middle East",
    "KW": "Middle East", "QA": "Middle East", "BH": "Middle East",
    "OM": "Middle East",
    # East Asia
    "CN": "East Asia", "JP": "East Asia", "KR": "East Asia",
    "KP": "East Asia", "TW": "East Asia", "MN": "East Asia",
    # South Asia
    "IN": "South Asia", "PK": "South Asia", "BD": "South Asia",
    "AF": "South Asia", "LK": "South Asia", "NP": "South Asia",
    # North America
    "US": "North America", "CA": "North America", "MX": "North America",
    # Sub-Saharan Africa
    "NG": "Sub-Saharan Africa", "KE": "Sub-Saharan Africa",
    "ZA": "Sub-Saharan Africa", "ET": "Sub-Saharan Africa",
    "GH": "Sub-Saharan Africa", "TZ": "Sub-Saharan Africa",
    "CD": "Sub-Saharan Africa", "SD": "Sub-Saharan Africa",
    "SO": "Sub-Saharan Africa",
}


# ---------------------------------------------------------------------------
# Haversine helper
# ---------------------------------------------------------------------------
def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Haversine distance in kilometres between two (lon, lat) points."""
    R = 6371.0
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _geo_centroid(record: NormalizedRecord) -> tuple[float, float] | None:
    """Extract (lon, lat) centroid from a record's geo field."""
    if record.geo is None or record.geo.coordinates is None:
        return None
    if record.geo.type == "Point" and len(record.geo.coordinates) >= 2:
        return (record.geo.coordinates[0], record.geo.coordinates[1])
    # For non-Point geometries, average all coordinate pairs (simplified centroid)
    coords = record.geo.coordinates
    if not coords:
        return None
    try:
        flat = _flatten_coords(coords)
        if not flat:
            return None
        avg_lon = sum(c[0] for c in flat) / len(flat)
        avg_lat = sum(c[1] for c in flat) / len(flat)
        return (avg_lon, avg_lat)
    except (TypeError, IndexError):
        return None


def _flatten_coords(coords: Any) -> list[tuple[float, float]]:
    """Recursively flatten nested coordinate arrays to (lon, lat) pairs."""
    if not coords:
        return []
    if isinstance(coords[0], (int, float)):
        return [(coords[0], coords[1])] if len(coords) >= 2 else []
    result: list[tuple[float, float]] = []
    for item in coords:
        result.extend(_flatten_coords(item))
    return result


def _extract_entity_ids(record: NormalizedRecord) -> set[str]:
    """Extract entity IDs from a record's payload using ENTITY_EXTRACTION_MAP."""
    config = ENTITY_EXTRACTION_MAP.get(int(record.tier))
    if config is None:
        return set()
    ids: set[str] = set()
    for fld in config.id_fields:
        val = record.payload.get(fld)
        if val is not None:
            if isinstance(val, list):
                ids.update(str(v) for v in val if v)
            else:
                ids.add(str(val))
    return ids


def _extract_entity_names(record: NormalizedRecord) -> set[str]:
    """Extract entity names (including aliases) from a record's payload."""
    config = ENTITY_EXTRACTION_MAP.get(int(record.tier))
    if config is None:
        return set()
    names: set[str] = set()
    for fld in config.name_fields:
        val = record.payload.get(fld)
        if val and isinstance(val, str):
            names.add(val.strip())
    for fld in config.alias_fields:
        val = record.payload.get(fld)
        if isinstance(val, list):
            names.update(str(v).strip() for v in val if v)
        elif val and isinstance(val, str):
            names.add(val.strip())
    return {n for n in names if n}


def _extract_keywords(record: NormalizedRecord) -> set[str]:
    """Extract significant keywords from payload text fields."""
    tokens: set[str] = set()
    for value in record.payload.values():
        if isinstance(value, str):
            words = re.split(r"\s+", value.lower())
            for w in words:
                cleaned = re.sub(r"[^a-z0-9]", "", w)
                if len(cleaned) > 3 and cleaned not in _STOPWORDS:
                    tokens.add(cleaned)
    return tokens


def _extract_country(record: NormalizedRecord) -> str | None:
    """Extract ISO 3166-1 alpha-2 country code from record."""
    # Priority 1: payload fields
    for fld in ("country_code", "country"):
        val = record.payload.get(fld)
        if val and isinstance(val, str):
            code = val.strip().upper()
            if len(code) == 2:
                return code
            # If it's a full country name, skip (we only handle codes)
    return None


def _geohash_prefix(lon: float, lat: float, precision: int = 4) -> str:
    """Compute a geohash prefix for coarse spatial filtering.

    Simplified geohash: divide the world into grid cells.
    Precision 4 ≈ 39km × 20km cells.
    """
    # Normalize to [0, 360) and [0, 180)
    norm_lon = (lon + 180.0) % 360.0
    norm_lat = lat + 90.0
    # Divide into cells
    cell_size_lon = 360.0 / (2 ** precision)
    cell_size_lat = 180.0 / (2 ** precision)
    cell_x = int(norm_lon / cell_size_lon)
    cell_y = int(norm_lat / cell_size_lat)
    return f"{cell_x}:{cell_y}"


def _adjacent_geohashes(prefix: str) -> set[str]:
    """Return the geohash prefix and its 8 neighbours."""
    parts = prefix.split(":")
    if len(parts) != 2:
        return {prefix}
    cx, cy = int(parts[0]), int(parts[1])
    neighbours: set[str] = set()
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            neighbours.add(f"{cx + dx}:{cy + dy}")
    return neighbours


# ---------------------------------------------------------------------------
# Abstract matcher
# ---------------------------------------------------------------------------
class BaseMatcher(ABC):
    """Abstract base for matching strategies."""

    @property
    @abstractmethod
    def dimension(self) -> str:
        """Match dimension name."""
        ...

    @abstractmethod
    def match(
        self, record_a: NormalizedRecord, record_b: NormalizedRecord
    ) -> MatchScore | None:
        """Evaluate match between two records. Returns MatchScore or None."""
        ...


# ---------------------------------------------------------------------------
# Concrete matchers
# ---------------------------------------------------------------------------
class SpatialProximityMatcher(BaseMatcher):
    """Haversine distance between record geo centroids."""

    dimension = "spatial"  # type: ignore[assignment]

    def __init__(self, max_distance_km: float = 50.0) -> None:
        self._max_km = max_distance_km

    def match(self, record_a: NormalizedRecord, record_b: NormalizedRecord) -> MatchScore | None:
        ca = _geo_centroid(record_a)
        cb = _geo_centroid(record_b)
        if ca is None or cb is None:
            return None
        dist = _haversine_km(ca[0], ca[1], cb[0], cb[1])
        if dist > self._max_km:
            return None
        score = 1.0 - (dist / self._max_km) if self._max_km > 0 else 1.0
        return MatchScore(
            dimension="spatial",
            score=max(score, 0.0),
            evidence={"distance_km": round(dist, 3)},
        )


class TemporalCooccurrenceMatcher(BaseMatcher):
    """Absolute time difference between record timestamps."""

    dimension = "temporal"  # type: ignore[assignment]

    def __init__(self, max_delta_s: float = 3600.0) -> None:
        self._max_delta = max_delta_s

    def match(self, record_a: NormalizedRecord, record_b: NormalizedRecord) -> MatchScore | None:
        delta = abs((record_a.timestamp - record_b.timestamp).total_seconds())
        if delta > self._max_delta:
            return None
        score = 1.0 - (delta / self._max_delta) if self._max_delta > 0 else 1.0
        return MatchScore(
            dimension="temporal",
            score=max(score, 0.0),
            evidence={"time_delta_s": round(delta, 1)},
        )


class EntityNameMatcher(BaseMatcher):
    """Jaro-Winkler similarity on entity names extracted from payloads."""

    dimension = "entity"  # type: ignore[assignment]

    def __init__(self, similarity_threshold: float = 0.85) -> None:
        self._threshold = similarity_threshold

    def match(self, record_a: NormalizedRecord, record_b: NormalizedRecord) -> MatchScore | None:
        names_a = _extract_entity_names(record_a)
        names_b = _extract_entity_names(record_b)
        if not names_a or not names_b:
            return None

        try:
            import jellyfish
        except ImportError:
            # Fallback: exact match only
            common = names_a & names_b
            if common:
                return MatchScore(
                    dimension="entity",
                    score=1.0,
                    evidence={"matched_names": list(common), "method": "exact"},
                )
            return None

        best_score = 0.0
        best_pair: tuple[str, str] = ("", "")
        for na in names_a:
            for nb in names_b:
                sim = jellyfish.jaro_winkler_similarity(na.lower(), nb.lower())
                if sim > best_score:
                    best_score = sim
                    best_pair = (na, nb)

        if best_score < self._threshold:
            return None
        return MatchScore(
            dimension="entity",
            score=best_score,
            evidence={
                "matched_names": list(best_pair),
                "similarity": round(best_score, 4),
                "method": "jaro_winkler",
            },
        )


class EntityIdMatcher(BaseMatcher):
    """Exact entity ID overlap across tiers."""

    dimension = "entity"  # type: ignore[assignment]

    def match(self, record_a: NormalizedRecord, record_b: NormalizedRecord) -> MatchScore | None:
        ids_a = _extract_entity_ids(record_a)
        ids_b = _extract_entity_ids(record_b)
        shared = ids_a & ids_b
        if not shared:
            return None
        return MatchScore(
            dimension="entity",
            score=1.0,
            evidence={"shared_ids": sorted(shared)},
        )


class TagOverlapMatcher(BaseMatcher):
    """Jaccard similarity on record tags."""

    dimension = "tag"  # type: ignore[assignment]

    def __init__(self, min_overlap: int = 2) -> None:
        self._min_overlap = min_overlap

    def match(self, record_a: NormalizedRecord, record_b: NormalizedRecord) -> MatchScore | None:
        tags_a = set(record_a.tags)
        tags_b = set(record_b.tags)
        intersection = tags_a & tags_b
        if len(intersection) < self._min_overlap:
            return None
        union = tags_a | tags_b
        score = len(intersection) / len(union) if union else 0.0
        return MatchScore(
            dimension="tag",
            score=score,
            evidence={"shared_tags": sorted(intersection), "jaccard": round(score, 4)},
        )


class KeywordCooccurrenceMatcher(BaseMatcher):
    """Keyword overlap from payload text fields."""

    dimension = "keyword"  # type: ignore[assignment]

    def __init__(self, min_shared_keywords: int = 3) -> None:
        self._min_shared = min_shared_keywords

    def match(self, record_a: NormalizedRecord, record_b: NormalizedRecord) -> MatchScore | None:
        kw_a = _extract_keywords(record_a)
        kw_b = _extract_keywords(record_b)
        shared = kw_a & kw_b
        if len(shared) < self._min_shared:
            return None
        denom = max(len(kw_a), len(kw_b))
        score = len(shared) / denom if denom > 0 else 0.0
        return MatchScore(
            dimension="keyword",
            score=min(score, 1.0),
            evidence={"shared_keywords": sorted(shared), "count": len(shared)},
        )


class GeographicRegionMatcher(BaseMatcher):
    """Country/region-level geographic matching."""

    dimension = "geographic_region"  # type: ignore[assignment]

    def match(self, record_a: NormalizedRecord, record_b: NormalizedRecord) -> MatchScore | None:
        country_a = _extract_country(record_a)
        country_b = _extract_country(record_b)
        if country_a is None or country_b is None:
            return None
        if country_a == country_b:
            return MatchScore(
                dimension="geographic_region",
                score=1.0,
                evidence={"country": country_a, "match_level": "country"},
            )
        # Check sub-region
        region_a = _SUBREGION_MAP.get(country_a)
        region_b = _SUBREGION_MAP.get(country_b)
        if region_a and region_b and region_a == region_b:
            return MatchScore(
                dimension="geographic_region",
                score=0.5,
                evidence={
                    "country_a": country_a,
                    "country_b": country_b,
                    "sub_region": region_a,
                    "match_level": "sub_region",
                },
            )
        return None
