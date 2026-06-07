"""Tests for the cross-tier mirror dedup resolver."""

from __future__ import annotations

from datetime import datetime, timezone

from hydra.mil_int.dedup import resolve_mirrors
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier


def _rec(source: str, url: str, tier: int = 100) -> NormalizedRecord:
    return NormalizedRecord(
        stream_id=source.lower().replace(" ", "-"),
        tier=Tier(tier),
        timestamp=datetime.now(timezone.utc),
        payload={"doc_url": url, "title": "x"},
        source_meta=SourceMeta(source_name=source, source_url="", adapter_type="doc_repo"),
        raw_hash="0" * 16,
    )


class TestResolveMirrors:
    def test_keeps_higher_authority(self):
        # DLA ASSIST outranks EverySpec.
        a = _rec("EverySpec", "http://everyspec.com/MIL-STD/MIL-STD-461.pdf")
        b = _rec("DLA ASSIST", "https://assist.dla.mil/quick/MIL-STD-461.pdf")
        result = resolve_mirrors([a, b])
        assert len(result.kept) == 1
        assert result.kept[0].source_meta.source_name == "DLA ASSIST"
        assert len(result.dropped) == 1

    def test_distinct_filenames_pass_through(self):
        a = _rec("EverySpec", "http://everyspec.com/MIL-STD/MIL-STD-461.pdf")
        b = _rec("EverySpec", "http://everyspec.com/MIL-STD/MIL-STD-810.pdf")
        result = resolve_mirrors([a, b])
        assert len(result.kept) == 2
        assert result.dropped == []

    def test_unknown_source_outranked_by_curated(self):
        a = _rec("Some Random Mirror", "https://x/MIL-STD-461.pdf")
        b = _rec("DTIC R&E Gateway", "https://discover.dtic.mil/MIL-STD-461.pdf")
        result = resolve_mirrors([a, b])
        assert len(result.kept) == 1
        assert result.kept[0].source_meta.source_name == "DTIC R&E Gateway"

    def test_no_doc_url_passes_through(self):
        rec = NormalizedRecord(
            stream_id="x",
            tier=Tier.MIL_INT_US_DOMESTIC,
            timestamp=datetime.now(timezone.utc),
            payload={"title": "no url"},
            source_meta=SourceMeta(source_name="X", source_url="", adapter_type="doc_repo"),
            raw_hash="0" * 16,
        )
        result = resolve_mirrors([rec])
        assert result.kept == [rec]
