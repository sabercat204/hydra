"""Composite confidence scoring for correlation pipelines."""

from __future__ import annotations

from hydra.correlation.models import MatchScore


class CompositeScorer:
    """Weighted composite scoring across match dimensions.

    Each pipeline declares dimension weights that sum to 1.0.
    The composite score is the weighted sum of individual dimension scores.
    Missing dimensions (matcher returned None) contribute 0.0.
    """

    def __init__(self, weights: dict[str, float]) -> None:
        total = sum(weights.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Weights must sum to 1.0, got {total}")
        self._weights = weights

    @property
    def weights(self) -> dict[str, float]:
        return dict(self._weights)

    def score(self, match_scores: list[MatchScore]) -> float:
        """Compute weighted composite confidence. Returns value in [0.0, 1.0]."""
        score_map = {ms.dimension: ms.score for ms in match_scores}
        composite = sum(
            self._weights.get(dim, 0.0) * score_map.get(dim, 0.0)
            for dim in self._weights
        )
        return min(max(composite, 0.0), 1.0)

    def score_with_convergence(
        self,
        match_scores: list[MatchScore],
        convergence_multiplier: float = 1.0,
    ) -> float:
        """Composite score with optional convergence bonus. Capped at 1.0."""
        base = self.score(match_scores)
        return min(base * convergence_multiplier, 1.0)
