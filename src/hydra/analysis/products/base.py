"""Abstract base for all intelligence product generators."""

from __future__ import annotations

from abc import ABC, abstractmethod

from hydra.analysis.models import DataBundle, IntelligenceProduct, ProductParams


class BaseProduct(ABC):
    """Abstract base for all intelligence product generators."""

    @property
    @abstractmethod
    def product_type(self) -> str:
        """Unique product type identifier."""
        ...

    @property
    @abstractmethod
    def source_tiers(self) -> list[int]:
        """Default tiers this product draws from."""
        ...

    @property
    @abstractmethod
    def requires_graph(self) -> bool:
        """Whether this product needs Neo4j graph data."""
        ...

    @property
    @abstractmethod
    def requires_timeline(self) -> bool:
        """Whether this product needs temporal event sequencing."""
        ...

    @property
    def default_lookback_hours(self) -> float:
        """Default time window lookback. Override per product."""
        return 24.0

    @abstractmethod
    async def generate(
        self, bundle: DataBundle, params: ProductParams
    ) -> IntelligenceProduct:
        """Generate the intelligence product from assembled data."""
        ...
