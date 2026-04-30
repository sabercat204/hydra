"""Tests for AuthManager — 14 tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

from hydra.auth.credential_store import YamlCredentialStore
from hydra.auth.exceptions import AuthConfigError
from hydra.auth.manager import AuthManager
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

def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fh:
        yaml.safe_dump(data, fh)


def _make_store(tmp_path: Path, creds: dict[str, Any]) -> YamlCredentialStore:
    p = tmp_path / "creds.yaml"
    _write_yaml(p, creds)
    return YamlCredentialStore(path=p)


# ---------------------------------------------------------------------------
# Dispatch tests (1-9)
# ---------------------------------------------------------------------------


def test_manager_dispatches_no_auth(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {})
    m = AuthManager("s", {"auth_pattern": "none"}, store)
    assert isinstance(m.strategy, NoAuthStrategy)


def test_manager_dispatches_api_key(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {"s": {"api_key": "k"}})
    m = AuthManager("s", {"auth_pattern": "api_key", "auth_key_location": "header", "auth_key_name": "X"}, store)
    assert isinstance(m.strategy, ApiKeyStrategy)


def test_manager_dispatches_basic_auth(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {"s": {"username": "u", "password": "p"}})
    m = AuthManager("s", {"auth_pattern": "basic_auth"}, store)
    assert isinstance(m.strategy, BasicAuthStrategy)


def test_manager_dispatches_oauth2(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {"s": {"client_id": "c", "client_secret": "s", "token_url": "https://a.com/t"}})
    m = AuthManager("s", {"auth_pattern": "oauth2_client_credentials"}, store)
    assert isinstance(m.strategy, OAuth2ClientCredentialsStrategy)


def test_manager_dispatches_cookie_auth(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {"s": {"cookie_name": "n", "cookie_value": "v"}})
    m = AuthManager("s", {"auth_pattern": "cookie_auth"}, store)
    assert isinstance(m.strategy, CookieAuthStrategy)


def test_manager_dispatches_rapidapi(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {"s": {"rapidapi_key": "k", "rapidapi_host": "h"}})
    m = AuthManager("s", {"auth_pattern": "rapidapi_key"}, store)
    assert isinstance(m.strategy, RapidApiKeyStrategy)


def test_manager_dispatches_certificate(tmp_path: Path) -> None:
    cert = tmp_path / "c.crt"
    key = tmp_path / "c.key"
    cert.write_text("cert")
    key.write_text("key")
    store = _make_store(tmp_path, {"s": {}})
    m = AuthManager("s", {"auth_pattern": "certificate", "cert_path": str(cert), "key_path": str(key)}, store)
    assert isinstance(m.strategy, CertificateAuthStrategy)


def test_manager_dispatches_account_token(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {"s": {"account_token": "tok"}})
    m = AuthManager("s", {"auth_pattern": "account_token", "auth_token_location": "header"}, store)
    assert isinstance(m.strategy, AccountTokenStrategy)


def test_manager_dispatches_aws_credentials(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {"s": {"aws_access_key_id": "A", "aws_secret_access_key": "S"}})
    m = AuthManager("s", {"auth_pattern": "aws_credentials"}, store)
    assert isinstance(m.strategy, AwsCredentialsStrategy)


# ---------------------------------------------------------------------------
# Error handling (10)
# ---------------------------------------------------------------------------


def test_manager_unknown_pattern_raises(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {"s": {}})
    with pytest.raises(AuthConfigError, match="unknown_value"):
        AuthManager("s", {"auth_pattern": "unknown_value"}, store)


# ---------------------------------------------------------------------------
# Delegation tests (11-13)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_manager_apply_delegates(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {})
    m = AuthManager("s", {"auth_pattern": "none"}, store)
    result = await m.apply()
    assert isinstance(result, AuthenticatedRequest)
    assert result.headers == {}


@pytest.mark.asyncio
async def test_manager_handle_401_stateful(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {"s": {"aws_access_key_id": "A", "aws_secret_access_key": "S"}})
    m = AuthManager("s", {"auth_pattern": "aws_credentials"}, store)
    assert m.strategy.is_stateful is True
    # AwsCredentialsStrategy.on_401 returns None (boto3 handles refresh)
    result = await m.handle_401()
    assert result is None


@pytest.mark.asyncio
async def test_manager_handle_401_stateless(tmp_path: Path) -> None:
    store = _make_store(tmp_path, {})
    m = AuthManager("s", {"auth_pattern": "none"}, store)
    result = await m.handle_401()
    assert result is None


# ---------------------------------------------------------------------------
# Reload test (14)
# ---------------------------------------------------------------------------


def test_manager_reload_credentials(tmp_path: Path) -> None:
    p = tmp_path / "creds.yaml"
    _write_yaml(p, {"s": {"api_key": "old"}})
    store = YamlCredentialStore(path=p)
    m = AuthManager("s", {"auth_pattern": "api_key", "auth_key_location": "header", "auth_key_name": "X"}, store)
    old_strategy = m.strategy

    _write_yaml(p, {"s": {"api_key": "new"}})
    m.reload_credentials()

    assert m.strategy is not old_strategy
    assert isinstance(m.strategy, ApiKeyStrategy)
