"""TimelineBuilder — temporal event sequencing from NormalizedRecords."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from hydra.analysis.models import EventCluster, TimelineEvent, TimelineResult
from hydra.config import HydraSettings
from hydra.correlation.models import CorrelationResult
from hydra.models.normalized import NormalizedRecord


class TimelineBuilder:
    """Constructs chronological event sequences from NormalizedRecords.

    Provides temporal analysis: event ordering, clustering,
    frequency analysis, and gap detection.
    """

    def __init__(self, settings: HydraSettings) -> None:
        self._cluster_window_s = getattr(
            getattr(settings, "analysis", None), "timeline_cluster_window_s", 3600.0
        )
        self._max_events = getattr(
            getattr(settings, "analysis", None), "timeline_max_events", 500
        )

    async def build(
        self,
        records: dict[int, list[NormalizedRecord]],
        correlations: list[CorrelationResult],
        time_start: str,
        time_end: str,
    ) -> TimelineResult:
        """Build timeline from records across tiers.

        Steps:
        1. Flatten all records into TimelineEvents.
        2. Sort by timestamp.
        3. Extract event title and description from payload.
        4. Annotate with correlation links.
        5. Score significance per event.
        6. If total events > max_events: keep top by significance.
        7. Detect temporal clusters.
        8. Return TimelineResult.
        """
        if not records:
            return TimelineResult(
                time_window_start=time_start,
                time_window_end=time_end,
            )

        # Build correlation lookup: raw_hash -> list of correlated hashes
        corr_map: dict[str, list[str]] = {}
        corr_hash_set: set[str] = set()
        for c in correlations:
            corr_map.setdefault(c.record_a_hash, []).append(c.record_b_hash)
            corr_map.setdefault(c.record_b_hash, []).append(c.record_a_hash)
            corr_hash_set.add(c.record_a_hash)
            corr_hash_set.add(c.record_b_hash)

        # 1-3. Flatten and extract
        events: list[TimelineEvent] = []
        tiers_seen: set[int] = set()
        for tier_id, tier_records in records.items():
            tiers_seen.add(tier_id)
            for rec in tier_records:
                geo_dict: dict | None = None
                if rec.geo:
                    geo_dict = {"type": rec.geo.type, "coordinates": rec.geo.coordinates}

                # 4. Correlation annotation
                correlated = corr_map.get(rec.raw_hash, [])

                # 5. Significance scoring
                significance = self._score_significance(rec, corr_hash_set)

                events.append(
                    TimelineEvent(
                        timestamp=rec.timestamp.isoformat() if isinstance(rec.timestamp, datetime) else str(rec.timestamp),
                        record_hash=rec.raw_hash,
                        tier=int(rec.tier),
                        stream_id=rec.stream_id,
                        title=self._extract_event_title(rec),
                        description=self._extract_event_description(rec),
                        geo=geo_dict,
                        significance=significance,
                        correlated_events=correlated,
                    )
                )

        # 2. Sort by timestamp
        events.sort(key=lambda e: e.timestamp)

        # 6. Cap at max_events by significance
        if len(events) > self._max_events:
            events.sort(key=lambda e: e.significance, reverse=True)
            events = events[: self._max_events]
            events.sort(key=lambda e: e.timestamp)

        # 7. Detect clusters
        clusters = self._detect_clusters(events)

        return TimelineResult(
            events=events,
            time_window_start=time_start,
            time_window_end=time_end,
            total_events=len(events),
            tiers_represented=sorted(tiers_seen),
            clusters=clusters,
        )

    def _extract_event_title(self, record: NormalizedRecord) -> str:
        """Tier-specific title extraction from payload."""
        p = record.payload
        tier = int(record.tier)

        if tier == 1:
            mag = p.get("magnitude", "?")
            place = p.get("place", "unknown location")
            return f"{mag} M earthquake at {place}"
        elif tier == 15:
            etype = p.get("event_type", p.get("type", "Event"))
            country = p.get("country", "unknown")
            notes = p.get("notes", "")
            title = f"{etype} in {country}"
            if notes:
                title += f": {notes[:80]}"
            return title
        elif tier == 16:
            stype = p.get("type", "Indicator")
            name = p.get("name", "unknown")
            return f"{stype}: {name}"
        elif tier == 18:
            vessel = p.get("vessel_name", p.get("callsign", "Unknown"))
            mmsi = p.get("mmsi", "")
            lat = p.get("lat", p.get("latitude", "?"))
            lon = p.get("lon", p.get("longitude", "?"))
            return f"{vessel} ({mmsi}) at {lat},{lon}"
        elif tier == 19:
            entity = p.get("entity_name", "Unknown entity")
            program = p.get("program", "")
            return f"{entity} — {program} listing" if program else f"{entity} — sanctions listing"
        else:
            ts = record.timestamp.isoformat() if isinstance(record.timestamp, datetime) else str(record.timestamp)
            return f"{record.stream_id} record at {ts}"

    def _extract_event_description(self, record: NormalizedRecord) -> str:
        """Tier-specific description extraction, truncated to 500 chars."""
        p = record.payload
        tier = int(record.tier)

        # Pick the most descriptive field per tier
        desc_fields = {
            1: ["place", "type", "status"],
            15: ["notes", "source", "description"],
            16: ["description", "pattern", "name"],
            18: ["destination", "status", "cargo"],
            19: ["remarks", "program", "source"],
            21: ["description", "summary", "notes"],
        }
        fields = desc_fields.get(tier, ["description", "summary", "notes", "text"])

        parts: list[str] = []
        for f in fields:
            val = p.get(f)
            if val and isinstance(val, str):
                parts.append(val)

        desc = "; ".join(parts) if parts else str(p)[:500]
        return desc[:500]

    def _score_significance(
        self,
        record: NormalizedRecord,
        correlated_hashes: set[str],
    ) -> float:
        """Score event significance.

        Events with correlations score higher.
        Base significance from record confidence.
        """
        base = record.confidence * 0.5
        corr_boost = 0.4 if record.raw_hash in correlated_hashes else 0.0
        # Anomaly factor — simple heuristic
        anomaly = self._anomaly_factor(record)
        return min(1.0, base + corr_boost + anomaly * 0.3)

    @staticmethod
    def _anomaly_factor(record: NormalizedRecord) -> float:
        """Tier-specific anomaly scoring."""
        p = record.payload
        tier = int(record.tier)

        if tier == 1:
            mag = p.get("magnitude", 0)
            if isinstance(mag, (int, float)) and mag > 4.0:
                return min(1.0, mag / 8.0)
            depth = p.get("depth_km", 100)
            if isinstance(depth, (int, float)) and depth < 10:
                return 0.6
        elif tier == 16:
            cvss = p.get("cvss", p.get("cvss_score", 0))
            if isinstance(cvss, (int, float)) and cvss > 7.0:
                return min(1.0, cvss / 10.0)
        elif tier == 15:
            fatalities = p.get("fatalities", 0)
            if isinstance(fatalities, (int, float)) and fatalities > 0:
                return min(1.0, 0.5 + fatalities / 100.0)
        return 0.5  # default: no anomaly detection for this tier yet

    def _detect_clusters(self, events: list[TimelineEvent]) -> list[EventCluster]:
        """Group temporally proximate events into clusters.

        Sliding window of cluster_window_s seconds.
        """
        if not events:
            return []

        clusters: list[EventCluster] = []
        current_cluster_events: list[TimelineEvent] = [events[0]]

        for i in range(1, len(events)):
            prev_ts = _parse_iso(current_cluster_events[0].timestamp)
            curr_ts = _parse_iso(events[i].timestamp)
            if prev_ts and curr_ts:
                diff = (curr_ts - prev_ts).total_seconds()
                if diff <= self._cluster_window_s:
                    current_cluster_events.append(events[i])
                    continue

            # Finalize current cluster if it has 2+ events
            if len(current_cluster_events) >= 2:
                clusters.append(self._make_cluster(current_cluster_events))
            current_cluster_events = [events[i]]

        # Final cluster
        if len(current_cluster_events) >= 2:
            clusters.append(self._make_cluster(current_cluster_events))

        return clusters

    @staticmethod
    def _make_cluster(events: list[TimelineEvent]) -> EventCluster:
        """Create an EventCluster from a group of events."""
        tiers = set(e.tier for e in events)
        tier_count = len(tiers)
        max_sig = max(e.significance for e in events)
        significance = max_sig * (1.0 + 0.1 * (tier_count - 1))

        # Centroid time — midpoint
        timestamps = [_parse_iso(e.timestamp) for e in events]
        valid_ts = [t for t in timestamps if t is not None]
        centroid_time = ""
        if valid_ts:
            avg_ts = valid_ts[0] + (valid_ts[-1] - valid_ts[0]) / 2
            centroid_time = avg_ts.isoformat()

        # Centroid geo — average of events with geo
        geo_events = [e for e in events if e.geo and e.geo.get("coordinates")]
        centroid_geo: dict | None = None
        if geo_events:
            lons = [e.geo["coordinates"][0] for e in geo_events]  # type: ignore[index]
            lats = [e.geo["coordinates"][1] for e in geo_events]  # type: ignore[index]
            centroid_geo = {
                "type": "Point",
                "coordinates": [sum(lons) / len(lons), sum(lats) / len(lats)],
            }

        return EventCluster(
            cluster_id=str(uuid.uuid4()),
            events=[e.record_hash for e in events],
            centroid_time=centroid_time,
            centroid_geo=centroid_geo,
            tier_count=tier_count,
            significance=min(1.0, significance),
        )


def _parse_iso(ts: str) -> datetime | None:
    """Parse ISO 8601 timestamp string."""
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None
