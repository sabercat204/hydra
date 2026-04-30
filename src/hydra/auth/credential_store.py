"""Abstract CredentialStore and YamlCredentialStore implementation."""

from __future__ import annotations

import abc
import copy
import threading
from pathlib import Path
from typing import Any

import structlog
import yaml

from .exceptions import CredentialNotFoundError, CredentialStoreError

logger = structlog.get_logger()


class CredentialStore(abc.ABC):
    """Pluggable backend for credential retrieval."""

    @abc.abstractmethod
    def get(self, stream_id: str) -> dict[str, Any]:
        """Retrieve credential dict for a stream.

        Raises ``CredentialNotFoundError`` if absent.
        """

    @abc.abstractmethod
    def reload(self) -> None:
        """Re-read the backing store."""

    @abc.abstractmethod
    def list_streams(self) -> list[str]:
        """Return all stream_ids with stored credentials."""


class YamlCredentialStore(CredentialStore):
    """Credential store backed by a local YAML file.

    Thread-safe via ``threading.Lock`` for concurrent ``reload`` / ``get`` calls.
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, Any]] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, stream_id: str) -> dict[str, Any]:
        """Return a *copy* of the credential dict for *stream_id*."""
        with self._lock:
            if stream_id not in self._data:
                raise CredentialNotFoundError(
                    f"No credentials found for stream '{stream_id}'",
                    stream_id=stream_id,
                )
            return copy.deepcopy(self._data[stream_id])

    def reload(self) -> None:
        """Re-read the YAML file from disk, replacing the in-memory dict."""
        self._load()
        logger.info("credential_reload", store_type="yaml", path=str(self._path))

    def list_streams(self) -> list[str]:
        """Return sorted list of all stream_ids with stored credentials."""
        with self._lock:
            return sorted(self._data.keys())

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Parse the YAML file into ``self._data``."""
        if not self._path.exists():
            raise CredentialStoreError(
                f"Credential file not found: {self._path}",
                stream_id="",
            )
        try:
            with open(self._path) as fh:
                raw = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise CredentialStoreError(
                f"Malformed YAML in credential file {self._path}: {exc}",
                stream_id="",
            ) from exc

        with self._lock:
            if raw is None:
                self._data = {}
            elif isinstance(raw, dict):
                self._data = {str(k): (v if isinstance(v, dict) else {}) for k, v in raw.items()}
            else:
                raise CredentialStoreError(
                    f"Credential file must be a YAML mapping, got {type(raw).__name__}",
                    stream_id="",
                )
