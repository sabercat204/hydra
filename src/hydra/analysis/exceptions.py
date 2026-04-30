"""Analysis-specific exceptions."""


class AnalysisError(Exception):
    """Base exception for analysis module."""


class ProductNotFoundError(AnalysisError):
    """Raised when a requested product_type is not registered."""

    def __init__(self, product_type: str):
        self.product_type = product_type
        super().__init__(f"Unknown product type: {product_type}")


class InsufficientDataError(AnalysisError):
    """Raised when insufficient data exists to generate a meaningful product."""

    def __init__(self, product_type: str, tiers_with_data: int, tiers_required: int):
        self.product_type = product_type
        self.tiers_with_data = tiers_with_data
        self.tiers_required = tiers_required
        super().__init__(
            f"Insufficient data for {product_type}: "
            f"{tiers_with_data}/{tiers_required} tiers have data"
        )


class EntityResolutionError(AnalysisError):
    """Raised when entity resolution fails to find any matching records."""

    def __init__(self, entity_id: str | None, entity_name: str | None):
        identifier = entity_id or entity_name or "unknown"
        super().__init__(f"Entity not found: {identifier}")


class QueryLayerError(AnalysisError):
    """Raised when an analytical query fails."""

    def __init__(self, engine: str, cause: str):
        self.engine = engine
        super().__init__(f"Query failed ({engine}): {cause}")


class GraphAnalysisError(AnalysisError):
    """Raised when Neo4j graph analysis fails."""

    def __init__(self, operation: str, cause: str):
        self.operation = operation
        super().__init__(f"Graph analysis failed ({operation}): {cause}")
