"""Auth-specific exception hierarchy.

Extends the adapter exception hierarchy from P1.
"""

from __future__ import annotations

from hydra.adapters.exceptions import AdapterError


class AuthError(AdapterError):
    """Base exception for all authentication failures."""

    def __init__(self, message: str, stream_id: str = "") -> None:
        self.stream_id = stream_id
        super().__init__(message)


class CredentialNotFoundError(AuthError):
    """Stream has no credentials in the store."""


class CredentialStoreError(AuthError):
    """Credential store backend failure (file not found, parse error, connection error)."""


class AuthConfigError(AuthError):
    """Stream registry auth fields invalid or missing."""


class TokenAcquisitionError(AuthError):
    """OAuth2/Earthdata token endpoint returned an error."""


class TokenRefreshError(AuthError):
    """Token refresh failed after 401."""
