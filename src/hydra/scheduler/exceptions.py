"""Scheduler-specific exception hierarchy."""

from __future__ import annotations


class SchedulerError(Exception):
    """Base exception for scheduler module."""


class BackpressureBlocked(SchedulerError):
    """Raised when WAQ depth exceeds hard limit — task should be skipped."""

    def __init__(self, engine: str, depth: int, hard_limit: int) -> None:
        self.engine = engine
        self.depth = depth
        self.hard_limit = hard_limit
        super().__init__(f"Backpressure BLOCKED: {engine} depth={depth} >= hard_limit={hard_limit}")


class ConcurrencyTimeout(SchedulerError):
    """Raised when concurrency slot acquisition times out."""

    def __init__(self, cadence: str, timeout: float) -> None:
        self.cadence = cadence
        self.timeout = timeout
        super().__init__(f"Concurrency timeout: {cadence} after {timeout}s")


class AdapterResolutionError(SchedulerError):
    """Raised when adapter_type string cannot be mapped to a concrete class."""

    def __init__(self, adapter_type: str) -> None:
        self.adapter_type = adapter_type
        super().__init__(f"Unknown adapter type: {adapter_type}")


class DeadStreamError(SchedulerError):
    """Raised when a stream exceeds the consecutive failure threshold."""

    def __init__(self, stream_id: str, consecutive_failures: int, threshold: int) -> None:
        self.stream_id = stream_id
        self.consecutive_failures = consecutive_failures
        self.threshold = threshold
        super().__init__(
            f"Dead stream: {stream_id} failed {consecutive_failures} consecutive runs "
            f"(threshold: {threshold})"
        )
