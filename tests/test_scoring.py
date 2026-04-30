"""Tests for correlation scoring module."""

import pytest

from hydra.correlation.models import MatchScore
from hydra.correlation.scoring import CompositeScorer


class TestCompositeScorer:
    """Tests for CompositeScorer."""

    def test_weights_sum_validation(self):
        """Weights not summing to 1.0 raises ValueError."""
        with pytest.raises(ValueError, match="Weights must sum to 1.0"):
            CompositeScorer(weights={"spatial": 0.5, "temporal": 0.3})

    def test_composite_score_all_dimensions(self):
        """All dimensions present → weighted sum."""
        scorer = CompositeScorer(weights={"spatial": 0.6, "temporal": 0.4})
        scores = [
            MatchScore(dimension="spatial", score=0.9),
            MatchScore(dimension="temporal", score=0.8),
        ]
        result = scorer.score(scores)
        expected = 0.6 * 0.9 + 0.4 * 0.8  # 0.54 + 0.32 = 0.86
        assert abs(result - expected) < 1e-9

    def test_composite_score_missing_dimension(self):
        """Missing dimension contributes 0.0."""
        scorer = CompositeScorer(weights={"spatial": 0.6, "temporal": 0.4})
        scores = [
            MatchScore(dimension="spatial", score=0.9),
        ]
        result = scorer.score(scores)
        expected = 0.6 * 0.9 + 0.4 * 0.0  # 0.54
        assert abs(result - expected) < 1e-9

    def test_convergence_multiplier_applied(self):
        """Base score × multiplier."""
        scorer = CompositeScorer(weights={"spatial": 0.6, "temporal": 0.4})
        scores = [
            MatchScore(dimension="spatial", score=0.7),
            MatchScore(dimension="temporal", score=0.6),
        ]
        base = scorer.score(scores)
        result = scorer.score_with_convergence(scores, convergence_multiplier=1.2)
        assert abs(result - min(base * 1.2, 1.0)) < 1e-9

    def test_score_clamped_0_1(self):
        """Output always in [0.0, 1.0]."""
        scorer = CompositeScorer(weights={"a": 1.0})
        # High multiplier should be capped
        scores = [MatchScore(dimension="a", score=0.95)]
        result = scorer.score_with_convergence(scores, convergence_multiplier=2.0)
        assert result == 1.0

        # Zero scores
        result = scorer.score([])
        assert result == 0.0

    def test_weights_near_one_accepted(self):
        """Weights summing to ~1.0 within tolerance are accepted."""
        scorer = CompositeScorer(weights={"a": 0.333, "b": 0.333, "c": 0.334})
        assert scorer is not None
