"""Tests for the doc_repo adapter (mil_int surface)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

# Prevent adapters/__init__ from pulling heavy deps that aren't installed.
sys.modules.setdefault("hydra.adapters.ckan", MagicMock())
sys.modules.setdefault("hydra.adapters.fdsn", MagicMock())
sys.modules.setdefault("hydra.adapters.odata", MagicMock())
sys.modules.setdefault("hydra.adapters.s3_bulk", MagicMock())
sys.modules.setdefault("hydra.adapters.sdmx", MagicMock())
sys.modules.setdefault("hydra.adapters.tap_vo", MagicMock())

from hydra.adapters.base import RawPayload
from hydra.adapters.doc_repo import DocRepoAdapter
from hydra.config import HydraSettings
from hydra.registry.stream_registry import StreamSource


SAMPLE_LISTING_HTML = b"""
<html><body>
<ul>
<li><a href="/pubs/sp800-53.pdf" title="SP 800-53 Rev 5">SP 800-53</a></li>
<li><a href="https://csrc.nist.gov/pubs/sp800-171.pdf">SP 800-171</a></li>
<li><a href="/pubs/sp800-53.pdf">SP 800-53 (duplicate)</a></li>
<li><a href="/about.html">About</a></li>
<li><a href="https://example.com/asset.png">Banner</a></li>
</ul>
</body></html>
"""


@pytest.fixture
def settings() -> HydraSettings:
    return HydraSettings()


def _make_adapter(
    settings: HydraSettings,
    *,
    list_url: str = "https://csrc.nist.gov/publications/sp800",
    access_policy: str = "open",
    extra_cfg: dict | None = None,
) -> DocRepoAdapter:
    src = StreamSource(
        name="NIST SP 800 Series",
        url=list_url,
        format="PDF",
        auth="none",
        notes="",
        access_policy=access_policy,
    )
    cfg = {"tier": 100, "country": "US", "content_type": "cybersecurity_frameworks"}
    if extra_cfg:
        cfg.update(extra_cfg)
    return DocRepoAdapter(
        stream_id="nist-sp-800",
        settings=settings,
        stream_config=cfg,
        registry_source=src,
    )


# ---------------------------------------------------------------------------
# parse / validate
# ---------------------------------------------------------------------------


class TestParseAnchors:
    def test_extracts_pdf_anchors(self, settings: HydraSettings):
        adapter = _make_adapter(settings)
        raw = RawPayload(
            stream_id="nist-sp-800",
            fetched_at=datetime.now(timezone.utc),
            content=SAMPLE_LISTING_HTML,
            content_type="text/html",
            http_status=200,
        )
        records = adapter.parse(raw)
        urls = {r["doc_url"] for r in records}
        assert "https://csrc.nist.gov/pubs/sp800-53.pdf" in urls
        assert "https://csrc.nist.gov/pubs/sp800-171.pdf" in urls
        # Image and dup must not appear.
        assert all(not u.endswith(".png") for u in urls)
        # Dup collapsed.
        assert sum(1 for u in urls if u.endswith("sp800-53.pdf")) == 1

    def test_max_docs_per_run_caps(self, settings: HydraSettings):
        adapter = _make_adapter(settings, extra_cfg={"max_docs_per_run": 1})
        raw = RawPayload(
            stream_id="nist-sp-800",
            fetched_at=datetime.now(timezone.utc),
            content=SAMPLE_LISTING_HTML,
            content_type="text/html",
            http_status=200,
        )
        assert len(adapter.parse(raw)) == 1

    def test_doc_url_pattern_overrides_extensions(self, settings: HydraSettings):
        adapter = _make_adapter(
            settings,
            extra_cfg={"doc_url_pattern": r"sp800-171", "doc_extensions": []},
        )
        raw = RawPayload(
            stream_id="nist-sp-800",
            fetched_at=datetime.now(timezone.utc),
            content=SAMPLE_LISTING_HTML,
            content_type="text/html",
            http_status=200,
        )
        records = adapter.parse(raw)
        urls = {r["doc_url"] for r in records}
        assert urls == {"https://csrc.nist.gov/pubs/sp800-171.pdf"}


class TestValidate:
    def test_drops_missing_doc_url(self, settings: HydraSettings):
        adapter = _make_adapter(settings)
        valid = adapter.validate(
            [
                {"doc_url": "https://x/y.pdf"},
                {"doc_url": ""},
                {"doc_url": "ftp://example.com/x.pdf"},
                {"doc_url": "https://x/y.pdf"},  # dup
            ]
        )
        assert [r["doc_url"] for r in valid] == ["https://x/y.pdf"]


# ---------------------------------------------------------------------------
# normalize
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_emits_normalized_records_with_tier_and_tags(self, settings: HydraSettings):
        adapter = _make_adapter(settings)
        records = adapter.normalize(
            [
                {
                    "doc_url": "https://csrc.nist.gov/pubs/sp800-53.pdf",
                    "title": "SP 800-53",
                    "list_url": "https://csrc.nist.gov/publications/sp800",
                    "discovered_at": "2026-06-06T00:00:00+00:00",
                }
            ]
        )
        assert len(records) == 1
        nr = records[0]
        assert int(nr.tier) == 100
        assert nr.payload["doc_url"].endswith("sp800-53.pdf")
        assert nr.payload["country"] == "US"
        assert nr.payload["content_type"] == "cybersecurity_frameworks"
        assert nr.payload["access_policy"] == "open"
        assert "mil_int" in nr.tags
        assert "NIST SP 800 Series" in nr.tags
        # raw_hash deterministic from doc_url.
        assert len(nr.raw_hash) == 16


# ---------------------------------------------------------------------------
# access policy gating
# ---------------------------------------------------------------------------


class TestAccessPolicyGate:
    @pytest.mark.parametrize(
        "policy",
        ["subscription", "restricted", "archived", "monitor_only"],
    )
    def test_non_open_short_circuits_fetch(
        self, settings: HydraSettings, policy: str
    ):
        adapter = _make_adapter(settings, access_policy=policy)
        import asyncio

        result = asyncio.run(adapter.fetch())
        assert result.http_status == 204
        assert result.content == b""
        assert adapter.parse(result) == []

    def test_registration_without_creds_skips(self, settings: HydraSettings):
        adapter = _make_adapter(settings, access_policy="registration")
        import asyncio

        result = asyncio.run(adapter.fetch())
        assert result.http_status == 204

    def test_registration_with_creds_is_ingestable(self, settings: HydraSettings):
        # Inject creds directly so _is_ingestable() returns True without
        # actually issuing an HTTP call.
        settings.credentials = {"nist-sp-800": {"api_key": "test-key"}}
        adapter = _make_adapter(settings, access_policy="registration")
        assert adapter._is_ingestable() is True


# ---------------------------------------------------------------------------
# adapter type registration
# ---------------------------------------------------------------------------


class TestAdapterRegistration:
    def test_adapter_type_is_doc_repo(self, settings: HydraSettings):
        assert DocRepoAdapter.adapter_type == "doc_repo"

    def test_task_runner_dispatch_includes_doc_repo(self):
        from hydra.scheduler.task_runner import _ADAPTER_TYPE_MAP

        assert _ADAPTER_TYPE_MAP["doc_repo"] == "hydra.adapters.doc_repo.DocRepoAdapter"
