"""HYDRA storage layer — routing, engines, health, and write-ahead buffer."""

from __future__ import annotations

__all__ = [
    "StorageRouter",
    "StorageEngine",
    "StoreResult",
    "StorageHealth",
    "StorageHealthAggregator",
    "RouteResult",
]


def __getattr__(name: str):
    if name in ("StorageEngine", "StoreResult"):
        from hydra.storage.engines.base import StorageEngine, StoreResult
        return {"StorageEngine": StorageEngine, "StoreResult": StoreResult}[name]
    if name in ("StorageHealth", "StorageHealthAggregator"):
        from hydra.storage.health import StorageHealth, StorageHealthAggregator
        return {"StorageHealth": StorageHealth, "StorageHealthAggregator": StorageHealthAggregator}[name]
    if name in ("StorageRouter", "RouteResult"):
        from hydra.storage.router import RouteResult, StorageRouter
        return {"StorageRouter": StorageRouter, "RouteResult": RouteResult}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
