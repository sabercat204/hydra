"""Unit tests for the TAP/VO adapter."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.adapters.exceptions import FetchError
from hydra.adapters.tap_vo import TapVoAdapter, _format_to_content_type
from hydra.adapters.base import RawPayload
from hydra.config import HydraSettings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURES = Path("tests/fixtures")

SAMPLE_CSV = (
    "ra,dec,mag,name\n"
    "180.0,-45.0,12.5,Star_A\n"
    "90.5,30.2,8.3,Star_B\n"
    "270.1,-10.0,15.1,Star_C\n"
    "45.0,60.0,,Star_D\n"
    "350.0,-80.5,20.0,Star_E\n"
)


def _make_adapter(cfg: dict[str, Any] | None = None) -> TapVoAdapter:
    settings = HydraSettings()
    with patch("hydra.adapters.base.get_registry") as mock_reg:
        mock_reg.return_value = MagicMock(tiers={})
        adapter = TapVoAdapter(
            stream_id="test_tap",
            settings=settings,
            stream_config=cfg or {},
        )
    return adapter


def _make_raw(content: bytes, content_type: str = "application/x-votable+xml", **extra_headers: str) -> RawPayload:
    return RawPayload(
        stream_id="test_tap",
        fetched_at=datetime.now(timezone.utc),
        content=content,
        content_type=content_type,
        http_status=200,
        headers={
            "tap_service_url": "https://example.com/tap/sync",
            "adql_query": "SELECT * FROM test",
            **extra_headers,
        },
    )


# ---------------------------------------------------------------------------
# Sync VOTable query
# ---------------------------------------------------------------------------


class TestSyncVotable:
    def test_parse_votable_fixture(self) -> None:
        """Parse the sample.vot fixture and verify row count, columns, null handling."""
        vot_bytes = FIXTURES.joinpath("sample.vot").read_bytes()
        adapter = _make_adapter({"response_format": "votable"})
        raw = _make_raw(vot_bytes)
        records = adapter.parse(raw)

        assert len(records) == 5
        assert records[0]["ra"] == 180.0
        assert records[0]["dec"] == -45.0
        assert records[0]["name"] == "Star_A"
        # Row 4 (Star_D) has empty mag → should be None
        assert records[3]["mag"] is None
        assert records[3]["name"] == "Star_D"
        # Provenance tags
        assert records[0]["_tap_service_url"] == "https://example.com/tap/sync"
        assert records[0]["_adql_query"] == "SELECT * FROM test"


# ---------------------------------------------------------------------------
# Sync CSV fallback
# ---------------------------------------------------------------------------


class TestSyncCsv:
    def test_parse_csv(self) -> None:
        """Parse CSV response and verify identical logical output."""
        adapter = _make_adapter({"response_format": "csv"})
        raw = _make_raw(SAMPLE_CSV.encode(), content_type="text/csv")
        records = adapter.parse(raw)

        assert len(records) == 5
        assert records[0]["ra"] == "180.0"
        assert records[0]["name"] == "Star_A"
        # Empty mag → None
        assert records[3]["mag"] is None


# ---------------------------------------------------------------------------
# Sync FITS response
# ---------------------------------------------------------------------------


class TestSyncFits:
    def test_parse_fits_fixture(self) -> None:
        """Parse the sample.fits fixture and verify row extraction."""
        fits_bytes = FIXTURES.joinpath("sample.fits").read_bytes()
        adapter = _make_adapter({"response_format": "fits"})
        raw = _make_raw(fits_bytes, content_type="application/fits")
        records = adapter.parse(raw)

        assert len(records) == 5
        assert records[0]["ra"] == 180.0
        assert records[0]["name"] == "Star_A"


# ---------------------------------------------------------------------------
# Async job lifecycle
# ---------------------------------------------------------------------------


class TestAsyncJobLifecycle:
    @pytest.mark.asyncio
    async def test_async_full_flow(self) -> None:
        """Mock the full async TAP flow: job creation → polling → result retrieval."""
        vot_bytes = FIXTURES.joinpath("sample.vot").read_bytes()

        adapter = _make_adapter({
            "tap_mode": "async",
            "base_url": "https://example.com",
            "tap_endpoint": "/tap/async",
            "adql_template": "SELECT * FROM {table_name}",
            "table_name": "test_table",
            "response_format": "votable",
            "async_poll_interval_seconds": 0.01,
            "async_max_wait_seconds": 10,
        })

        # Mock aiohttp session
        phase_calls = iter(["PENDING", "EXECUTING", "COMPLETED"])

        mock_job_resp = AsyncMock()
        mock_job_resp.status = 303
        mock_job_resp.headers = {"Location": "https://example.com/tap/async/job123"}
        mock_job_resp.__aenter__ = AsyncMock(return_value=mock_job_resp)
        mock_job_resp.__aexit__ = AsyncMock(return_value=False)

        mock_phase_resp = AsyncMock()
        mock_phase_resp.text = AsyncMock(side_effect=lambda: next(phase_calls))
        mock_phase_resp.__aenter__ = AsyncMock(return_value=mock_phase_resp)
        mock_phase_resp.__aexit__ = AsyncMock(return_value=False)

        mock_result_resp = AsyncMock()
        mock_result_resp.read = AsyncMock(return_value=vot_bytes)
        mock_result_resp.content_type = "application/x-votable+xml"
        mock_result_resp.status = 200
        mock_result_resp.headers = {}
        mock_result_resp.__aenter__ = AsyncMock(return_value=mock_result_resp)
        mock_result_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_job_resp)
        mock_session.get = MagicMock(side_effect=lambda url, **kw: (
            mock_phase_resp if "/phase" in url else mock_result_resp
        ))
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            raw = await adapter.fetch()

        assert raw.content == vot_bytes
        assert raw.http_status == 200


# ---------------------------------------------------------------------------
# Async job error
# ---------------------------------------------------------------------------


class TestAsyncJobError:
    @pytest.mark.asyncio
    async def test_async_error_phase(self) -> None:
        """Verify FetchError raised when async job transitions to ERROR."""
        adapter = _make_adapter({
            "tap_mode": "async",
            "base_url": "https://example.com",
            "tap_endpoint": "/tap/async",
            "adql_template": "SELECT * FROM {table_name}",
            "table_name": "test_table",
            "async_poll_interval_seconds": 0.01,
        })

        mock_job_resp = AsyncMock()
        mock_job_resp.status = 303
        mock_job_resp.headers = {"Location": "https://example.com/tap/async/job456"}
        mock_job_resp.__aenter__ = AsyncMock(return_value=mock_job_resp)
        mock_job_resp.__aexit__ = AsyncMock(return_value=False)

        mock_phase_resp = AsyncMock()
        mock_phase_resp.text = AsyncMock(return_value="ERROR")
        mock_phase_resp.__aenter__ = AsyncMock(return_value=mock_phase_resp)
        mock_phase_resp.__aexit__ = AsyncMock(return_value=False)

        mock_error_resp = AsyncMock()
        mock_error_resp.text = AsyncMock(return_value="Query syntax error near line 1")
        mock_error_resp.__aenter__ = AsyncMock(return_value=mock_error_resp)
        mock_error_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_job_resp)
        mock_session.get = MagicMock(side_effect=lambda url, **kw: (
            mock_phase_resp if "/phase" in url else mock_error_resp
        ))
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            with pytest.raises(FetchError, match="error"):
                await adapter.fetch()


# ---------------------------------------------------------------------------
# Async poll timeout
# ---------------------------------------------------------------------------


class TestAsyncPollTimeout:
    @pytest.mark.asyncio
    async def test_async_timeout(self) -> None:
        """Verify adapter attempts job deletion and raises FetchError on timeout."""
        adapter = _make_adapter({
            "tap_mode": "async",
            "base_url": "https://example.com",
            "tap_endpoint": "/tap/async",
            "adql_template": "SELECT * FROM {table_name}",
            "table_name": "test_table",
            "async_poll_interval_seconds": 0.01,
            "async_max_wait_seconds": 0.02,
        })

        mock_job_resp = AsyncMock()
        mock_job_resp.status = 303
        mock_job_resp.headers = {"Location": "https://example.com/tap/async/job789"}
        mock_job_resp.__aenter__ = AsyncMock(return_value=mock_job_resp)
        mock_job_resp.__aexit__ = AsyncMock(return_value=False)

        mock_phase_resp = AsyncMock()
        mock_phase_resp.text = AsyncMock(return_value="EXECUTING")
        mock_phase_resp.__aenter__ = AsyncMock(return_value=mock_phase_resp)
        mock_phase_resp.__aexit__ = AsyncMock(return_value=False)

        mock_delete_resp = AsyncMock()
        mock_delete_resp.__aenter__ = AsyncMock(return_value=mock_delete_resp)
        mock_delete_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_job_resp)
        mock_session.get = MagicMock(return_value=mock_phase_resp)
        mock_session.delete = MagicMock(return_value=mock_delete_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            with pytest.raises(FetchError, match="timed out"):
                await adapter.fetch()

        # Verify delete was attempted
        mock_session.delete.assert_called()


# ---------------------------------------------------------------------------
# ADQL template resolution
# ---------------------------------------------------------------------------


class TestAdqlTemplateResolution:
    def test_resolve_adql(self) -> None:
        """Verify ADQL template variable substitution."""
        adapter = _make_adapter({
            "adql_template": "SELECT TOP {max_rows} * FROM {table_name} WHERE modified > '{last_fetch_time}' {custom_where}",
            "table_name": "ps",
            "max_rows": 500,
            "custom_where": "AND dec > -30",
        })

        result = adapter._resolve_adql("2026-01-01T00:00:00Z")
        assert "TOP 500" in result
        assert "FROM ps" in result
        assert "2026-01-01T00:00:00Z" in result
        assert "AND dec > -30" in result


# ---------------------------------------------------------------------------
# Coordinate validation
# ---------------------------------------------------------------------------


class TestCoordinateValidation:
    def test_invalid_ra_dropped(self) -> None:
        """Records with ra=400 (out of [0,360]) should be dropped."""
        adapter = _make_adapter({
            "coordinate_fields": {"ra": "ra", "dec": "dec"},
        })
        records = [
            {"ra": 400, "dec": 10, "name": "bad"},
            {"ra": 180, "dec": 45, "name": "good"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["name"] == "good"

    def test_invalid_dec_dropped(self) -> None:
        """Records with dec=100 (out of [-90,90]) should be dropped."""
        adapter = _make_adapter({
            "coordinate_fields": {"ra": "ra", "dec": "dec"},
        })
        records = [
            {"ra": 180, "dec": 100, "name": "bad"},
            {"ra": 180, "dec": 45, "name": "good"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["name"] == "good"


# ---------------------------------------------------------------------------
# Numeric range validation
# ---------------------------------------------------------------------------


class TestNumericRangeValidation:
    def test_out_of_range_dropped(self) -> None:
        """Records with mag=-5 outside [0,30] should be dropped."""
        adapter = _make_adapter({
            "numeric_ranges": {"mag": [0, 30]},
        })
        records = [
            {"mag": -5, "name": "bad"},
            {"mag": 15, "name": "good"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["name"] == "good"


# ---------------------------------------------------------------------------
# Deduplication with composite key
# ---------------------------------------------------------------------------


class TestDeduplication:
    def test_composite_key_dedup(self) -> None:
        """Two records with identical dedup_key_fields values → only one passes."""
        adapter = _make_adapter({
            "dedup_key_fields": ["pl_name", "disc_year"],
        })
        records = [
            {"pl_name": "Kepler-22b", "disc_year": 2011, "ra": 286.0},
            {"pl_name": "Kepler-22b", "disc_year": 2011, "ra": 286.1},
            {"pl_name": "Kepler-442b", "disc_year": 2015, "ra": 115.0},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 2


# ---------------------------------------------------------------------------
# Auth token injection
# ---------------------------------------------------------------------------


class TestAuthTokenInjection:
    def test_cookie_auth(self) -> None:
        """Verify cookie set for account_token auth via cookie location."""
        settings = HydraSettings()
        settings.credentials = {"gaia_dr3": {"token": "my_secret_token"}}  # type: ignore[attr-defined]

        with patch("hydra.adapters.base.get_registry") as mock_reg:
            mock_reg.return_value = MagicMock(tiers={})
            adapter = TapVoAdapter(
                stream_id="gaia_dr3",
                settings=settings,
                stream_config={
                    "auth_pattern": "account_token",
                    "auth_token_location": "cookie",
                },
            )

        headers = adapter._build_auth_headers()
        assert "Cookie" in headers
        assert "my_secret_token" in headers["Cookie"]

    def test_header_auth(self) -> None:
        """Verify Authorization header set for account_token auth via header location."""
        settings = HydraSettings()
        settings.credentials = {"gaia_dr3": {"token": "my_secret_token"}}  # type: ignore[attr-defined]

        with patch("hydra.adapters.base.get_registry") as mock_reg:
            mock_reg.return_value = MagicMock(tiers={})
            adapter = TapVoAdapter(
                stream_id="gaia_dr3",
                settings=settings,
                stream_config={
                    "auth_pattern": "account_token",
                    "auth_token_location": "header",
                },
            )

        headers = adapter._build_auth_headers()
        assert "Authorization" in headers
        assert "Bearer my_secret_token" in headers["Authorization"]


# ---------------------------------------------------------------------------
# Configuration-driven behavior
# ---------------------------------------------------------------------------


class TestConfigDrivenBehavior:
    def test_different_configs_produce_different_urls(self) -> None:
        """Two different registry entries produce different fetch URLs and modes."""
        exoplanet_cfg = {
            "base_url": "https://exoplanetarchive.ipac.caltech.edu",
            "tap_endpoint": "/TAP/sync",
            "tap_mode": "sync",
            "adql_template": "SELECT TOP {max_rows} * FROM {table_name}",
            "table_name": "ps",
            "response_format": "votable",
        }
        heasarc_cfg = {
            "base_url": "https://heasarc.gsfc.nasa.gov",
            "tap_endpoint": "/tap/async",
            "tap_mode": "async",
            "adql_template": "SELECT * FROM {table_name} WHERE time > '{last_fetch_time}'",
            "table_name": "xray_master",
            "response_format": "csv",
        }

        exo_adapter = _make_adapter(exoplanet_cfg)
        heasarc_adapter = _make_adapter(heasarc_cfg)

        assert exo_adapter._get_cfg("tap_mode") == "sync"
        assert heasarc_adapter._get_cfg("tap_mode") == "async"
        assert exo_adapter._get_cfg("response_format") == "votable"
        assert heasarc_adapter._get_cfg("response_format") == "csv"

        exo_adql = exo_adapter._resolve_adql()
        heasarc_adql = heasarc_adapter._resolve_adql("2026-01-01T00:00:00Z")
        assert "ps" in exo_adql
        assert "xray_master" in heasarc_adql
        assert "2026-01-01T00:00:00Z" in heasarc_adql
