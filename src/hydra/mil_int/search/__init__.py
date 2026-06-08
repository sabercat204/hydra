"""Search backends for the mil_int surface.

The :class:`SearchBackend` protocol is the contract every backend must
satisfy. :class:`ElasticsearchSearchBackend` is the production
implementation; :class:`InMemorySearchBackend` is the deterministic
fallback used by tests and development environments without an ES
cluster.
"""

from hydra.mil_int.search.backend import SearchBackend
from hydra.mil_int.search.elasticsearch import ElasticsearchSearchBackend
from hydra.mil_int.search.memory import InMemorySearchBackend

__all__ = [
    "ElasticsearchSearchBackend",
    "InMemorySearchBackend",
    "SearchBackend",
]
