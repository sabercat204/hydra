"""Abstract AuthStrategy and 9 concrete strategy implementations."""

from __future__ import annotations

import abc
import asyncio
import base64
import ssl
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiohttp
import structlog

from .exceptions import (
    AuthConfigError,
    CredentialNotFoundError,
    TokenAcquisitionError,
    TokenRefreshError,
)

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# AuthenticatedRequest — output of every strategy.apply()
# ---------------------------------------------------------------------------

@dataclass
class AuthenticatedRequest:
    """Auth context to merge into an outbound HTTP request."""

    headers: dict[str, str] = field(default_factory=dict)
    query_params: dict[str, str] = field(default_factory=dict)
    cookies: dict[str, str] = field(default_factory=dict)
    ssl_context: ssl.SSLContext | None = None
    boto3_config: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class AuthStrategy(abc.ABC):
    """Abstract base for all auth strategies."""

    def __init__(
        self,
        stream_id: str,
        stream_config: dict[str, Any],
        credentials: dict[str, Any],
    ) -> None:
        self.stream_id = stream_id
        self.stream_config = stream_config
        self.credentials = credentials

    @property
    @abc.abstractmethod
    def is_stateful(self) -> bool:
        """True if the strategy manages token lifecycle."""

    @abc.abstractmethod
    async def apply(self) -> AuthenticatedRequest:
        """Produce the auth context for an outbound request."""

    @abc.abstractmethod
    async def on_401(self) -> AuthenticatedRequest | None:
        """Handle HTTP 401. Stateful strategies refresh; stateless return None."""


# ---------------------------------------------------------------------------
# 4.6.1  NoAuthStrategy
# ---------------------------------------------------------------------------

class NoAuthStrategy(AuthStrategy):
    """Pattern ``none`` — no authentication required."""

    @property
    def is_stateful(self) -> bool:
        return False

    async def apply(self) -> AuthenticatedRequest:
        return AuthenticatedRequest()

    async def on_401(self) -> AuthenticatedRequest | None:
        return None


# ---------------------------------------------------------------------------
# 4.6.2  ApiKeyStrategy
# ---------------------------------------------------------------------------

class ApiKeyStrategy(AuthStrategy):
    """Pattern ``api_key`` — inject key into header or query param."""

    def __init__(self, stream_id: str, stream_config: dict[str, Any], credentials: dict[str, Any]) -> None:
        super().__init__(stream_id, stream_config, credentials)
        # Validate
        location = stream_config.get("auth_key_location")
        if location not in ("header", "query"):
            raise AuthConfigError(
                f"Stream '{stream_id}': auth_key_location must be 'header' or 'query', got '{location}'",
                stream_id=stream_id,
            )
        key_name = stream_config.get("auth_key_name")
        if not key_name:
            raise AuthConfigError(
                f"Stream '{stream_id}': auth_key_name is required for api_key pattern",
                stream_id=stream_id,
            )
        if "api_key" not in credentials:
            raise CredentialNotFoundError(
                f"Stream '{stream_id}': credential 'api_key' not found",
                stream_id=stream_id,
            )
        self._location: str = location
        self._key_name: str = key_name
        self._api_key: str = credentials["api_key"]

    @property
    def is_stateful(self) -> bool:
        return False

    async def apply(self) -> AuthenticatedRequest:
        if self._location == "header":
            return AuthenticatedRequest(headers={self._key_name: self._api_key})
        return AuthenticatedRequest(query_params={self._key_name: self._api_key})

    async def on_401(self) -> AuthenticatedRequest | None:
        return None


# ---------------------------------------------------------------------------
# 4.6.3  BasicAuthStrategy
# ---------------------------------------------------------------------------

class BasicAuthStrategy(AuthStrategy):
    """Pattern ``basic_auth`` — HTTP Basic authentication."""

    def __init__(self, stream_id: str, stream_config: dict[str, Any], credentials: dict[str, Any]) -> None:
        super().__init__(stream_id, stream_config, credentials)
        if "username" not in credentials:
            raise CredentialNotFoundError(
                f"Stream '{stream_id}': credential 'username' not found",
                stream_id=stream_id,
            )
        if "password" not in credentials:
            raise CredentialNotFoundError(
                f"Stream '{stream_id}': credential 'password' not found",
                stream_id=stream_id,
            )
        self._username: str = credentials["username"]
        self._password: str = credentials["password"]

    @property
    def is_stateful(self) -> bool:
        return False

    async def apply(self) -> AuthenticatedRequest:
        raw = f"{self._username}:{self._password}"
        encoded = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        return AuthenticatedRequest(headers={"Authorization": f"Basic {encoded}"})

    async def on_401(self) -> AuthenticatedRequest | None:
        return None


# ---------------------------------------------------------------------------
# 4.6.4  OAuth2ClientCredentialsStrategy
# ---------------------------------------------------------------------------

class OAuth2ClientCredentialsStrategy(AuthStrategy):
    """Pattern ``oauth2_client_credentials`` — acquire/cache/refresh bearer tokens."""

    def __init__(self, stream_id: str, stream_config: dict[str, Any], credentials: dict[str, Any]) -> None:
        super().__init__(stream_id, stream_config, credentials)
        for key in ("client_id", "client_secret", "token_url"):
            if key not in credentials:
                raise CredentialNotFoundError(
                    f"Stream '{stream_id}': credential '{key}' not found",
                    stream_id=stream_id,
                )
        parsed = urlparse(credentials["token_url"])
        if parsed.scheme != "https":
            raise AuthConfigError(
                f"Stream '{stream_id}': token_url must use HTTPS, got '{parsed.scheme}'",
                stream_id=stream_id,
            )
        self._client_id: str = credentials["client_id"]
        self._client_secret: str = credentials["client_secret"]
        self._token_url: str = credentials["token_url"]
        self._access_token: str | None = None
        self._token_expiry: datetime | None = None
        self._lock = asyncio.Lock()

    @property
    def is_stateful(self) -> bool:
        return True

    async def apply(self) -> AuthenticatedRequest:
        async with self._lock:
            now = datetime.now(timezone.utc)
            if self._access_token is None or (
                self._token_expiry is not None and now >= self._token_expiry
            ):
                await self._acquire_token()
        return AuthenticatedRequest(headers={"Authorization": f"Bearer {self._access_token}"})

    async def on_401(self) -> AuthenticatedRequest | None:
        async with self._lock:
            self._access_token = None
            try:
                await self._acquire_token()
            except TokenAcquisitionError as exc:
                raise TokenRefreshError(
                    f"Stream '{self.stream_id}': token refresh failed: {exc}",
                    stream_id=self.stream_id,
                ) from exc
        return AuthenticatedRequest(headers={"Authorization": f"Bearer {self._access_token}"})

    async def _acquire_token(self) -> None:
        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self._token_url, data=data) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        raise TokenAcquisitionError(
                            f"Stream '{self.stream_id}': token endpoint returned {resp.status}: {body}",
                            stream_id=self.stream_id,
                        )
                    payload = await resp.json()
        except TokenAcquisitionError:
            raise
        except Exception as exc:
            raise TokenAcquisitionError(
                f"Stream '{self.stream_id}': token acquisition failed: {exc}",
                stream_id=self.stream_id,
            ) from exc

        access_token = payload.get("access_token")
        if not access_token:
            raise TokenAcquisitionError(
                f"Stream '{self.stream_id}': token response missing 'access_token'",
                stream_id=self.stream_id,
            )
        expires_in = int(payload.get("expires_in", 3600))
        self._access_token = access_token
        self._token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in) - timedelta(seconds=60)
        logger.info("token_acquire", stream_id=self.stream_id, token_url=self._token_url, expires_in=expires_in)


# ---------------------------------------------------------------------------
# 4.6.5  CookieAuthStrategy
# ---------------------------------------------------------------------------

class CookieAuthStrategy(AuthStrategy):
    """Pattern ``cookie_auth`` — inject a session cookie."""

    def __init__(self, stream_id: str, stream_config: dict[str, Any], credentials: dict[str, Any]) -> None:
        super().__init__(stream_id, stream_config, credentials)
        if "cookie_name" not in credentials:
            raise CredentialNotFoundError(
                f"Stream '{stream_id}': credential 'cookie_name' not found",
                stream_id=stream_id,
            )
        if "cookie_value" not in credentials:
            raise CredentialNotFoundError(
                f"Stream '{stream_id}': credential 'cookie_value' not found",
                stream_id=stream_id,
            )
        self._cookie_name: str = credentials["cookie_name"]
        self._cookie_value: str = credentials["cookie_value"]

    @property
    def is_stateful(self) -> bool:
        return False

    async def apply(self) -> AuthenticatedRequest:
        return AuthenticatedRequest(cookies={self._cookie_name: self._cookie_value})

    async def on_401(self) -> AuthenticatedRequest | None:
        return None


# ---------------------------------------------------------------------------
# 4.6.6  RapidApiKeyStrategy
# ---------------------------------------------------------------------------

class RapidApiKeyStrategy(AuthStrategy):
    """Pattern ``rapidapi_key`` — inject RapidAPI headers."""

    def __init__(self, stream_id: str, stream_config: dict[str, Any], credentials: dict[str, Any]) -> None:
        super().__init__(stream_id, stream_config, credentials)
        if "rapidapi_key" not in credentials:
            raise CredentialNotFoundError(
                f"Stream '{stream_id}': credential 'rapidapi_key' not found",
                stream_id=stream_id,
            )
        if "rapidapi_host" not in credentials:
            raise CredentialNotFoundError(
                f"Stream '{stream_id}': credential 'rapidapi_host' not found",
                stream_id=stream_id,
            )
        self._key: str = credentials["rapidapi_key"]
        self._host: str = credentials["rapidapi_host"]

    @property
    def is_stateful(self) -> bool:
        return False

    async def apply(self) -> AuthenticatedRequest:
        return AuthenticatedRequest(headers={
            "X-RapidAPI-Key": self._key,
            "X-RapidAPI-Host": self._host,
        })

    async def on_401(self) -> AuthenticatedRequest | None:
        return None


# ---------------------------------------------------------------------------
# 4.6.7  CertificateAuthStrategy
# ---------------------------------------------------------------------------

class CertificateAuthStrategy(AuthStrategy):
    """Pattern ``certificate`` — mutual TLS via client certificate."""

    def __init__(self, stream_id: str, stream_config: dict[str, Any], credentials: dict[str, Any]) -> None:
        super().__init__(stream_id, stream_config, credentials)
        cert_path = stream_config.get("cert_path")
        key_path = stream_config.get("key_path")
        if not cert_path:
            raise AuthConfigError(
                f"Stream '{stream_id}': cert_path is required for certificate pattern",
                stream_id=stream_id,
            )
        if not key_path:
            raise AuthConfigError(
                f"Stream '{stream_id}': key_path is required for certificate pattern",
                stream_id=stream_id,
            )
        if not Path(cert_path).exists():
            raise AuthConfigError(
                f"Stream '{stream_id}': cert_path not found: {cert_path}",
                stream_id=stream_id,
            )
        if not Path(key_path).exists():
            raise AuthConfigError(
                f"Stream '{stream_id}': key_path not found: {key_path}",
                stream_id=stream_id,
            )
        self._cert_path: str = cert_path
        self._key_path: str = key_path
        self._ssl_context: ssl.SSLContext | None = None

    @property
    def is_stateful(self) -> bool:
        return False

    async def apply(self) -> AuthenticatedRequest:
        if self._ssl_context is None:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.load_cert_chain(certfile=self._cert_path, keyfile=self._key_path)
            self._ssl_context = ctx
            logger.debug("ssl_context_loaded", stream_id=self.stream_id, cert_path=self._cert_path)
        return AuthenticatedRequest(ssl_context=self._ssl_context)

    async def on_401(self) -> AuthenticatedRequest | None:
        return None


# ---------------------------------------------------------------------------
# 4.6.8  AccountTokenStrategy
# ---------------------------------------------------------------------------

class AccountTokenStrategy(AuthStrategy):
    """Pattern ``account_token`` — static token or Earthdata OAuth2 flow."""

    def __init__(self, stream_id: str, stream_config: dict[str, Any], credentials: dict[str, Any]) -> None:
        super().__init__(stream_id, stream_config, credentials)
        self._is_earthdata = False
        self._static_token: str | None = None
        self._token_location: str = "header"
        self._access_token: str | None = None
        self._token_expiry: datetime | None = None
        self._lock = asyncio.Lock()

        if "account_token" in credentials:
            # Static variant
            token = credentials["account_token"]
            if not token:
                raise CredentialNotFoundError(
                    f"Stream '{stream_id}': account_token is empty",
                    stream_id=stream_id,
                )
            location = stream_config.get("auth_token_location", "header")
            if location not in ("header", "cookie"):
                raise AuthConfigError(
                    f"Stream '{stream_id}': auth_token_location must be 'header' or 'cookie', got '{location}'",
                    stream_id=stream_id,
                )
            self._static_token = token
            self._token_location = location
        elif "client_id" in credentials and "username" in credentials:
            # Earthdata OAuth2 variant
            for key in ("client_id", "client_secret", "username", "password"):
                if key not in credentials:
                    raise CredentialNotFoundError(
                        f"Stream '{stream_id}': credential '{key}' not found for Earthdata variant",
                        stream_id=stream_id,
                    )
            self._is_earthdata = True
            self._client_id: str = credentials["client_id"]
            self._client_secret: str = credentials["client_secret"]
            self._username: str = credentials["username"]
            self._password: str = credentials["password"]
        else:
            raise CredentialNotFoundError(
                f"Stream '{stream_id}': account_token credentials must contain 'account_token' "
                "or ('client_id' + 'username') for Earthdata variant",
                stream_id=stream_id,
            )

    @property
    def is_stateful(self) -> bool:
        return self._is_earthdata

    async def apply(self) -> AuthenticatedRequest:
        if not self._is_earthdata:
            # Static token
            assert self._static_token is not None
            if self._token_location == "header":
                return AuthenticatedRequest(headers={"Authorization": f"Bearer {self._static_token}"})
            return AuthenticatedRequest(cookies={"account_token": self._static_token})

        # Earthdata OAuth2 flow
        async with self._lock:
            now = datetime.now(timezone.utc)
            if self._access_token is None or (
                self._token_expiry is not None and now >= self._token_expiry
            ):
                await self._acquire_earthdata_token()
        return AuthenticatedRequest(headers={"Authorization": f"Bearer {self._access_token}"})

    async def on_401(self) -> AuthenticatedRequest | None:
        if not self._is_earthdata:
            return None
        async with self._lock:
            self._access_token = None
            try:
                await self._acquire_earthdata_token()
            except TokenAcquisitionError as exc:
                raise TokenRefreshError(
                    f"Stream '{self.stream_id}': Earthdata token refresh failed: {exc}",
                    stream_id=self.stream_id,
                ) from exc
        return AuthenticatedRequest(headers={"Authorization": f"Bearer {self._access_token}"})

    async def _acquire_earthdata_token(self) -> None:
        token_url = "https://urs.earthdata.nasa.gov/oauth/authorize"
        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "username": self._username,
            "password": self._password,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(token_url, data=data) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        raise TokenAcquisitionError(
                            f"Stream '{self.stream_id}': Earthdata token endpoint returned {resp.status}: {body}",
                            stream_id=self.stream_id,
                        )
                    payload = await resp.json()
        except TokenAcquisitionError:
            raise
        except Exception as exc:
            raise TokenAcquisitionError(
                f"Stream '{self.stream_id}': Earthdata token acquisition failed: {exc}",
                stream_id=self.stream_id,
            ) from exc

        access_token = payload.get("access_token")
        if not access_token:
            raise TokenAcquisitionError(
                f"Stream '{self.stream_id}': Earthdata token response missing 'access_token'",
                stream_id=self.stream_id,
            )
        expires_in = int(payload.get("expires_in", 3600))
        self._access_token = access_token
        self._token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in) - timedelta(seconds=60)
        logger.info("token_acquire", stream_id=self.stream_id, token_url=token_url, expires_in=expires_in)


# ---------------------------------------------------------------------------
# 4.6.9  AwsCredentialsStrategy
# ---------------------------------------------------------------------------

class AwsCredentialsStrategy(AuthStrategy):
    """Pattern ``aws_credentials`` — pass AWS creds for boto3 client construction."""

    def __init__(self, stream_id: str, stream_config: dict[str, Any], credentials: dict[str, Any]) -> None:
        super().__init__(stream_id, stream_config, credentials)
        if "aws_access_key_id" not in credentials:
            raise CredentialNotFoundError(
                f"Stream '{stream_id}': credential 'aws_access_key_id' not found",
                stream_id=stream_id,
            )
        if "aws_secret_access_key" not in credentials:
            raise CredentialNotFoundError(
                f"Stream '{stream_id}': credential 'aws_secret_access_key' not found",
                stream_id=stream_id,
            )
        self._config = {
            "aws_access_key_id": credentials["aws_access_key_id"],
            "aws_secret_access_key": credentials["aws_secret_access_key"],
            "aws_session_token": credentials.get("aws_session_token"),
        }

    @property
    def is_stateful(self) -> bool:
        return True

    async def apply(self) -> AuthenticatedRequest:
        return AuthenticatedRequest(boto3_config=self._config)

    async def on_401(self) -> AuthenticatedRequest | None:
        return None
