"""Abstract StorageEngine interface and StoreResult dataclass."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from hydra.models.normalized import NormalizedRecord
from hydra.storage.health import StorageHealth


@dataclass
class StoreResult:
    """Result of a batch write to a storage engine."""

    engine: str
    stored: int = 0
    failed: int = 0
    deduplicated: int = 0
    duration_ms: float = 0.0
    errors: list[dict] = field(default_factory=list)


class StorageEngine(ABC):
    """Abstract base for all storage engine implementations."""

    @abstractmethod
    async def store(self, records: list[NormalizedRecord]) -> StoreResult:
        """Write a batch of records to the engine.

        Returns StoreResult with counts of successful, failed, and deduplicated writes.
        Raises StorageEngineError on unrecoverable failures.
        """
        ...

    @abstractmethod
    async def health_check(self) -> StorageHealth:
        ...

    @abstractmethod
    async def connect(self) -> None:
        """Initialize connection pool. Called once at startup."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully close connection pool. Called at shutdown."""
        ...
