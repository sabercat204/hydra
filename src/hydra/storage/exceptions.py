"""Storage-specific exception hierarchy."""

from __future__ import annotations


class StorageError(Exception):
    """Base exception for all storage layer errors."""
    pass


class StorageEngineError(StorageError):
    """Unrecoverable error from a specific engine."""

    def __init__(self, engine: str, message: str, cause: Exception | None = None):
        self.engine = engine
        self.cause = cause
        super().__init__(f"[{engine}] {message}")


class StorageConnectionError(StorageEngineError):
    """Engine connection failure."""
    pass


class StorageWriteError(StorageEngineError):
    """Write operation failure."""
    pass


class StorageSerializationError(StorageEngineError):
    """Record serialization failure before write."""
    pass


class DedupCacheError(StorageError):
    """Redis dedup cache failure. Non-fatal — PG safety net catches duplicates."""
    pass


class QueueError(StorageError):
    """Write-ahead queue or DLQ operation failure."""
    pass


class ReconciliationError(StorageError):
    """DLQ reconciliation failure."""
    pass
