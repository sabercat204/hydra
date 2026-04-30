"""AuthManager — single entry point for all adapter auth operations."""

from __future__ import annotations

from typing import Any

import structlog

from .credential_store import CredentialStore
from .exceptions import AuthConfigError, CredentialNotFoundError
from .strategies import (
    AccountTokenStrategy,
    ApiKeyStrategy,
    AuthenticatedRequest,
    AuthStrategy,
    AwsCredentialsStrategy,
    BasicAuthStrategy,
    CertificateAuthStrategy,
    CookieAuthStrategy,
    NoAuthStrategy,
    OAuth2ClientCredentialsStrategy,
    RapidApiKeyStrategy,
)

logger = structlog.get_logger()

# Re-export AuthenticatedRequest from this module as specified in §4.2
__all__ = ["AuthManager", "AuthenticatedRequest"]

_STRATEGY_REGISTRY: dict[str, type[AuthStrategy]] = {
    "none": NoAuthStrategy,
    "api_key": ApiKeyStrategy,
    "basic_auth": BasicAuthStrategy,
    "oauth2_client_credentials": OAuth2ClientCredentialsStrategy,
    "cookie_auth": CookieAuthStrategy,
    "rapidapi_key": RapidApiKeyStrategy,
    "certificate": CertificateAuthStrategy,
    "account_token": AccountTokenStrategy,
    "aws_credentials": AwsCredentialsStrategy,
}


class AuthManager:
    """Dispatch layer that resolves credentials and delegates to the correct AuthStrategy.

    One ``AuthManager`` instance per adapter instance.
    """

    def __init__(
        self,
        stream_id: str,
        stream_config: dict[str, Any],
        credential_store: CredentialStore,
    ) -> None:
        self.stream_id = stream_id
        self._stream_config = stream_config
        self._credential_store = credential_store
        self._auth_pattern: str = stream_config.get("auth_pattern", "none")
        self.strategy: AuthStrategy = self._build_strategy()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def apply(self) -> AuthenticatedRequest:
        """Produce auth context for an outbound request."""
        logger.debug(
            "auth_apply",
            stream_id=self.stream_id,
            auth_pattern=self._auth_pattern,
            strategy_class=type(self.strategy).__name__,
        )
        return await self.strategy.apply()

    async def handle_401(self) -> AuthenticatedRequest | None:
        """Handle HTTP 401. Returns refreshed auth context or None."""
        return await self.strategy.on_401()

    def reload_credentials(self) -> None:
        """Re-read credential store and re-initialize the strategy."""
        self._credential_store.reload()
        self.strategy = self._build_strategy()
        logger.info("credential_reload", stream_id=self.stream_id, store_type="yaml")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_strategy(self) -> AuthStrategy:
        strategy_cls = _STRATEGY_REGISTRY.get(self._auth_pattern)
        if strategy_cls is None:
            raise AuthConfigError(
                f"Stream '{self.stream_id}': unknown auth_pattern '{self._auth_pattern}'",
                stream_id=self.stream_id,
            )

        # Resolve credentials — for 'none' pattern, credentials may be absent
        try:
            credentials = self._credential_store.get(self.stream_id)
        except CredentialNotFoundError:
            if self._auth_pattern == "none":
                credentials = {}
            else:
                raise

        return strategy_cls(
            stream_id=self.stream_id,
            stream_config=self._stream_config,
            credentials=credentials,
        )
