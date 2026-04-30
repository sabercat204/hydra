"""Correlation-specific exceptions."""


class CorrelationError(Exception):
    """Base exception for correlation module."""


class PipelineNotFoundError(CorrelationError):
    """Raised when a requested pipeline_id is not registered."""

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        super().__init__(f"Unknown pipeline: {pipeline_id}")


class CandidateQueryError(CorrelationError):
    """Raised when candidate record query fails."""

    def __init__(self, pipeline_id: str, cause: str):
        self.pipeline_id = pipeline_id
        super().__init__(f"Candidate query failed for {pipeline_id}: {cause}")


class PersistenceError(CorrelationError):
    """Raised when correlation result persistence fails."""

    def __init__(self, target: str, cause: str):
        self.target = target
        super().__init__(f"Persistence failed ({target}): {cause}")


class MatcherError(CorrelationError):
    """Raised when a matcher encounters an unrecoverable error."""

    def __init__(self, dimension: str, cause: str):
        self.dimension = dimension
        super().__init__(f"Matcher error ({dimension}): {cause}")


class TriggerThrottledError(CorrelationError):
    """Raised when a correlation trigger is throttled by min_trigger_interval."""

    def __init__(self, pipeline_id: str, elapsed_s: float, min_interval_s: float):
        self.pipeline_id = pipeline_id
        super().__init__(
            f"Trigger throttled: {pipeline_id} last ran {elapsed_s:.0f}s ago "
            f"(min interval: {min_interval_s:.0f}s)"
        )
