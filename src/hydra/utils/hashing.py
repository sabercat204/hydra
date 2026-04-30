"""Content hashing utilities for deduplication."""

import xxhash


def compute_raw_hash(data: bytes) -> str:
    """Return 16-char xxhash64 hex digest of *data*."""
    return xxhash.xxh64(data).hexdigest()
