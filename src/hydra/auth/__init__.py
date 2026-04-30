"""HYDRA authentication layer.

Exports:
    AuthManager — single entry point for adapter auth
    AuthenticatedRequest — dataclass merged into outbound requests
    AuthStrategy — abstract strategy interface
    CredentialStore — abstract credential backend interface
    YamlCredentialStore — YAML-file credential store
"""

from .credential_store import CredentialStore, YamlCredentialStore
from .manager import AuthenticatedRequest, AuthManager
from .strategies import AuthStrategy

__all__ = [
    "AuthManager",
    "AuthenticatedRequest",
    "AuthStrategy",
    "CredentialStore",
    "YamlCredentialStore",
]
