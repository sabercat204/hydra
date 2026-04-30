"""Abstract base for all correlation pipelines."""

from __future__ import annotations

from abc import ABC, abstractmethod

from hydra.correlation.models import CandidateSet, CorrelationResult


class BasePipeline(ABC):
    """Abstract base for all correlation pipelines."""

    @property
    @abstractmethod
    def pipeline_id(self) -> str:
        """Unique pipeline identifier matching stream_registry.yaml."""
        ...

    @property
    @abstractmethod
    def source_tiers(self) -> list[int]:
        """Tiers this pipeline correlates across."""
        ...

    @abstractmethod
    async def correlate(
        self, candidates: CandidateSet
    ) -> list[CorrelationResult]:
        """Execute correlation logic on candidate records.

        Returns list of discovered correlations above the confidence threshold.
        """
        ...

    @property
    def confidence_threshold(self) -> float:
        """Minimum composite confidence to emit a result. Default: 0.5."""
        return 0.5

    @property
    def max_pairs_per_run(self) -> int:
        """Safety cap on pair evaluations per run. Default: 100,000."""
        return 100_000
