"""Tests for all AuthStrategy implementations — 32 tests."""

from __future__ import annotations

import base64
import ssl
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.auth.exceptions import (
    AuthConfigError,
    CredentialNotFoundError,
    TokenAcquisitionError,
)
from hydra.auth.strategies import (
    AccountTokenStrategy,
    ApiKeyStrategy,
    AuthenticatedRequest,
    AwsCredentialsStrategy,
    BasicAuthStrategy,
    CertificateAuthStrategy,
    CookieAuthStrategy,
    NoAuthStrategy,
    OAuth2ClientCredentialsStrategy,
    RapidApiKeyStrategy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg(**kw: Any) -> dict[str, Any]:
    """Build a minimal stream config dict."""
    return dict(kw)


# ---------------------------------------------------------------------------
# NoAuthStrategy (2 tests)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_auth_apply_returns_empty() -> None:
    s = NoAuthStrategy("s", {}, {})
    result = await s.apply()
    assert result.headers == {}
    assert result.query_params == {}
    assert result.cookies == {}
    assert result.ssl_context is None
    assert result.boto3_config is None


@pytest.mark.asyncio
async def test_no_auth_on_401_returns_none() -> None:
    s = NoAuthStrategy("s", {}, {})
    assert await s.on_401() is None


# ---------------------------------------------------------------------------
# ApiKeyStrategy (5 tests)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_api_key_header_injection() -> None:
    s = ApiKeyStrategy(
        "s",
        _cfg(auth_key_location="header", auth_key_name="X-API-Key"),
        {"api_key": "secret123"},
    )
    result = await s.apply()
    assert result.headers == {"X-API-Key": "secret123"}
    assert result.query_params == {}


@pytest.mark.asyncio
async def test_api_key_query_injection() -> None:
    s = ApiKeyStrategy(
        "s",
        _cfg(auth_key_location="query", auth_key_name="apikey"),
        {"api_key": "qval"},
    )
    result = await s.apply()
    assert result.query_params == {"apikey": "qval"}
    assert result.headers == {}


def test_api_key_missing_credential() -> None:
    with pytest.raises(CredentialNotFoundError):
        ApiKeyStrategy("s", _cfg(auth_key_location="header", auth_key_name="X"), {})


def test_api_key_missing_location() -> None:
    with pytest.raises(AuthConfigError):
        ApiKeyStrategy("s", _cfg(auth_key_name="X"), {"api_key": "k"})


@pytest.mark.asyncio
async def test_api_key_on_401_returns_none() -> None:
    s = ApiKeyStrategy("s", _cfg(auth_key_location="header", auth_key_name="X"), {"api_key": "k"})
    assert await s.on_401() is None


# ---------------------------------------------------------------------------
# BasicAuthStrategy (3 tests)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_basic_auth_header_encoding() -> None:
    s = BasicAuthStrategy("s", {}, {"username": "user", "password": "pass"})
    result = await s.apply()
    expected = base64.b64encode(b"user:pass").decode("ascii")
    assert result.headers == {"Authorization": f"Basic {expected}"}


def test_basic_auth_missing_username() -> None:
    with pytest.raises(CredentialNotFoundError):
        BasicAuthStrategy("s", {}, {"password": "p"})


@pytest.mark.asyncio
async def test_basic_auth_special_characters() -> None:
    s = BasicAuthStrategy("s", {}, {"username": "u:ser", "password": "p@ss:wörd"})
    result = await s.apply()
    expected = base64.b64encode("u:ser:p@ss:wörd".encode("utf-8")).decode("ascii")
    assert result.headers["Authorization"] == f"Basic {expected}"


# ---------------------------------------------------------------------------
# OAuth2ClientCredentialsStrategy (8 tests)
# ---------------------------------------------------------------------------


def _mock_token_response(status: int = 200, json_data: dict | None = None, text: str = "") -> AsyncMock:
    """Create a mock aiohttp response."""
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data or {"access_token": "tok123", "expires_in": 3600})
    resp.text = AsyncMock(return_value=text)
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


def _mock_session(resp: AsyncMock) -> AsyncMock:
    session = AsyncMock()
    session.post = MagicMock(return_value=resp)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session


_OAUTH2_CREDS = {
    "client_id": "cid",
    "client_secret": "csec",
    "token_url": "https://auth.example.com/token",
}


@pytest.mark.asyncio
async def test_oauth2_initial_token_acquisition() -> None:
    s = OAuth2ClientCredentialsStrategy("s", {}, _OAUTH2_CREDS)
    resp = _mock_token_response()
    session = _mock_session(resp)
    with patch("hydra.auth.strategies.aiohttp.ClientSession", return_value=session):
        result = await s.apply()
    assert result.headers["Authorization"] == "Bearer tok123"


@pytest.mark.asyncio
async def test_oauth2_cached_token_reuse() -> None:
    s = OAuth2ClientCredentialsStrategy("s", {}, _OAUTH2_CREDS)
    resp = _mock_token_response()
    session = _mock_session(resp)
    with patch("hydra.auth.strategies.aiohttp.ClientSession", return_value=session):
        await s.apply()
        await s.apply()
    # post should be called only once (token cached)
    session.post.assert_called_once()


@pytest.mark.asyncio
async def test_oauth2_proactive_refresh() -> None:
    s = OAuth2ClientCredentialsStrategy("s", {}, _OAUTH2_CREDS)
    # First call — acquire token with very short expiry
    resp1 = _mock_token_response(json_data={"access_token": "tok1", "expires_in": 30})
    session1 = _mock_session(resp1)
    with patch("hydra.auth.strategies.aiohttp.ClientSession", return_value=session1):
        await s.apply()

    # Token expiry is 30s - 60s buffer = already expired → should re-acquire
    resp2 = _mock_token_response(json_data={"access_token": "tok2", "expires_in": 3600})
    session2 = _mock_session(resp2)
    with patch("hydra.auth.strategies.aiohttp.ClientSession", return_value=session2):
        result = await s.apply()
    assert result.headers["Authorization"] == "Bearer tok2"


@pytest.mark.asyncio
async def test_oauth2_on_401_refreshes_token() -> None:
    s = OAuth2ClientCredentialsStrategy("s", {}, _OAUTH2_CREDS)
    resp = _mock_token_response(json_data={"access_token": "refreshed", "expires_in": 3600})
    session = _mock_session(resp)
    with patch("hydra.auth.strategies.aiohttp.ClientSession", return_value=session):
        result = await s.on_401()
    assert result is not None
    assert result.headers["Authorization"] == "Bearer refreshed"


@pytest.mark.asyncio
async def test_oauth2_token_endpoint_error() -> None:
    s = OAuth2ClientCredentialsStrategy("s", {}, _OAUTH2_CREDS)
    resp = _mock_token_response(status=400, text="bad request")
    session = _mock_session(resp)
    with patch("hydra.auth.strategies.aiohttp.ClientSession", return_value=session):
        with pytest.raises(TokenAcquisitionError, match="400"):
            await s.apply()


@pytest.mark.asyncio
async def test_oauth2_token_endpoint_missing_access_token() -> None:
    s = OAuth2ClientCredentialsStrategy("s", {}, _OAUTH2_CREDS)
    resp = _mock_token_response(json_data={"token_type": "bearer"})  # no access_token
    session = _mock_session(resp)
    with patch("hydra.auth.strategies.aiohttp.ClientSession", return_value=session):
        with pytest.raises(TokenAcquisitionError, match="missing"):
            await s.apply()


def test_oauth2_missing_credentials() -> None:
    with pytest.raises(CredentialNotFoundError):
        OAuth2ClientCredentialsStrategy("s", {}, {"client_secret": "x", "token_url": "https://a.com/t"})


def test_oauth2_token_url_not_https() -> None:
    with pytest.raises(AuthConfigError, match="HTTPS"):
        OAuth2ClientCredentialsStrategy("s", {}, {"client_id": "c", "client_secret": "s", "token_url": "http://a.com/t"})


# ---------------------------------------------------------------------------
# CookieAuthStrategy (2 tests)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cookie_auth_sets_cookie() -> None:
    s = CookieAuthStrategy("s", {}, {"cookie_name": "sid", "cookie_value": "abc"})
    result = await s.apply()
    assert result.cookies == {"sid": "abc"}


def test_cookie_auth_missing_cookie_name() -> None:
    with pytest.raises(CredentialNotFoundError):
        CookieAuthStrategy("s", {}, {"cookie_value": "abc"})


# ---------------------------------------------------------------------------
# RapidApiKeyStrategy (2 tests)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rapidapi_sets_headers() -> None:
    s = RapidApiKeyStrategy("s", {}, {"rapidapi_key": "rk", "rapidapi_host": "host.example.com"})
    result = await s.apply()
    assert result.headers == {"X-RapidAPI-Key": "rk", "X-RapidAPI-Host": "host.example.com"}


def test_rapidapi_missing_host() -> None:
    with pytest.raises(CredentialNotFoundError):
        RapidApiKeyStrategy("s", {}, {"rapidapi_key": "rk"})


# ---------------------------------------------------------------------------
# CertificateAuthStrategy (4 tests)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_certificate_ssl_context_created(tmp_path: Path) -> None:
    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("cert")
    key.write_text("key")

    s = CertificateAuthStrategy("s", {"cert_path": str(cert), "key_path": str(key)}, {})
    with patch.object(ssl.SSLContext, "load_cert_chain"):
        result = await s.apply()
    assert result.ssl_context is not None


@pytest.mark.asyncio
async def test_certificate_ssl_context_cached(tmp_path: Path) -> None:
    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("cert")
    key.write_text("key")

    s = CertificateAuthStrategy("s", {"cert_path": str(cert), "key_path": str(key)}, {})
    with patch.object(ssl.SSLContext, "load_cert_chain"):
        r1 = await s.apply()
        r2 = await s.apply()
    assert r1.ssl_context is r2.ssl_context


def test_certificate_missing_cert_path() -> None:
    with pytest.raises(AuthConfigError, match="cert_path"):
        CertificateAuthStrategy("s", {"key_path": "/some/key"}, {})


def test_certificate_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(AuthConfigError, match="not found"):
        CertificateAuthStrategy("s", {"cert_path": str(tmp_path / "nope.crt"), "key_path": str(tmp_path / "nope.key")}, {})


# ---------------------------------------------------------------------------
# AccountTokenStrategy (4 tests)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_account_token_static_header() -> None:
    s = AccountTokenStrategy("s", {"auth_token_location": "header"}, {"account_token": "tok"})
    result = await s.apply()
    assert result.headers == {"Authorization": "Bearer tok"}


@pytest.mark.asyncio
async def test_account_token_static_cookie() -> None:
    s = AccountTokenStrategy("s", {"auth_token_location": "cookie"}, {"account_token": "tok"})
    result = await s.apply()
    assert result.cookies == {"account_token": "tok"}


@pytest.mark.asyncio
async def test_account_token_earthdata_flow() -> None:
    creds = {"client_id": "cid", "client_secret": "csec", "username": "u", "password": "p"}
    s = AccountTokenStrategy("s", {}, creds)
    assert s.is_stateful is True

    resp = _mock_token_response(json_data={"access_token": "earth_tok", "expires_in": 3600})
    session = _mock_session(resp)
    with patch("hydra.auth.strategies.aiohttp.ClientSession", return_value=session):
        result = await s.apply()
    assert result.headers["Authorization"] == "Bearer earth_tok"


def test_account_token_variant_detection() -> None:
    # Static variant
    s1 = AccountTokenStrategy("s", {"auth_token_location": "header"}, {"account_token": "t"})
    assert s1.is_stateful is False

    # Earthdata variant
    s2 = AccountTokenStrategy("s", {}, {"client_id": "c", "client_secret": "s", "username": "u", "password": "p"})
    assert s2.is_stateful is True

    # Neither → error
    with pytest.raises(CredentialNotFoundError):
        AccountTokenStrategy("s", {}, {"some_other_key": "v"})


# ---------------------------------------------------------------------------
# AwsCredentialsStrategy (2 tests)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aws_credentials_boto3_config() -> None:
    creds = {
        "aws_access_key_id": "AKIA...",
        "aws_secret_access_key": "secret",
        "aws_session_token": "session",
    }
    s = AwsCredentialsStrategy("s", {}, creds)
    result = await s.apply()
    assert result.boto3_config == {
        "aws_access_key_id": "AKIA...",
        "aws_secret_access_key": "secret",
        "aws_session_token": "session",
    }


def test_aws_credentials_missing_access_key() -> None:
    with pytest.raises(CredentialNotFoundError):
        AwsCredentialsStrategy("s", {}, {"aws_secret_access_key": "s"})
