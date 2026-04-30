"""Monitoring subsystem exception hierarchy.

These exceptions are caught within background collection and computation
loops, logged, and never propagated to crash the application. See
design.md §"Exception Hierarchy" and requirement 22.5.
"""

from __future__ import annotations


class MonitoringError(Exception):
    """Base exception for the monitoring subsystem."""


class CollectorError(MonitoringError):
    """Raised when a metric collector fails to gather metrics.

    Attributes:
        collector_name: Name of the collector that raised the error.
    """

    def __init__(self, collector_name: str, message: str) -> None:
        self.collector_name = collector_name
        super().__init__(f"[{collector_name}] {message}")


class AnomalyDetectionError(MonitoringError):
    """Raised when an anomaly detection computation fails."""


class CapacityPlanningError(MonitoringError):
    """Raised when a capacity planning computation fails."""


class SLOComputationError(MonitoringError):
    """Raised when SLO or error budget computation fails."""
