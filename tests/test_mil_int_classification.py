"""Tests for the mil_int classification gate."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hydra.mil_int.classification import (
    detect_classification,
    filter_unclassified,
    is_unclassified,
)
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier


def _make_record(
    *, title: str = "Open document", tags: list[str] | None = None,
    classification: str | None = None,
) -> NormalizedRecord:
    payload: dict = {"title": title, "doc_url": "https://example.com/x.pdf"}
    if classification is not None:
        payload["classification"] = classification
    return NormalizedRecord(
        stream_id="t",
        tier=Tier.MIL_INT_US_DOMESTIC,
        timestamp=datetime.now(timezone.utc),
        payload=payload,
        source_meta=SourceMeta(source_name="t", source_url="", adapter_type="doc_repo"),
        raw_hash="0" * 16,
        tags=tags or [],
    )


class TestDetectClassification:
    @pytest.mark.parametrize(
        "text",
        [
            "CONFIDENTIAL",
            "SECRET//NOFORN",
            "(TS//SCI) Foo",
            "FOUO Memo",
            "NATO RESTRICTED Doc",
            "Confidential lowercase",
        ],
    )
    def test_detects_markers(self, text: str):
        assert detect_classification(text) is not None

    @pytest.mark.parametrize(
        "text",
        ["UNCLASSIFIED", "Public release", "NIST SP 800-53", "MIL-STD-461"],
    )
    def test_no_marker_for_clean_text(self, text: str):
        assert detect_classification(text) is None


class TestIsUnclassified:
    def test_clean_record_passes(self):
        ok, marker = is_unclassified(_make_record())
        assert ok is True
        assert marker is None

    def test_explicit_classification_field_blocks(self):
        ok, marker = is_unclassified(_make_record(classification="SECRET"))
        assert ok is False
        assert marker == "SECRET"

    def test_marker_in_title_blocks(self):
        ok, marker = is_unclassified(_make_record(title="(TS//SCI) doc"))
        assert ok is False
        assert "TS//SCI" in (marker or "")

    def test_marker_in_tags_blocks(self):
        ok, marker = is_unclassified(_make_record(tags=["NOFORN"]))
        assert ok is False


class TestFilterUnclassified:
    def test_drops_classified_records(self):
        good = _make_record()
        bad = _make_record(classification="SECRET")
        out = filter_unclassified([good, bad])
        assert out == [good]

    def test_log_only_admits_violations(self):
        bad = _make_record(classification="CONFIDENTIAL")
        out = filter_unclassified([bad], log_only=True)
        assert out == [bad]

    def test_enforce_false_passes_all_through(self):
        bad = _make_record(classification="SECRET")
        out = filter_unclassified([bad], enforce=False)
        assert out == [bad]

    def test_invokes_violation_callback(self):
        seen: list[str] = []
        bad = _make_record(classification="TOP SECRET")
        filter_unclassified([bad], on_violation=seen.append)
        assert seen == ["TOP SECRET"]
